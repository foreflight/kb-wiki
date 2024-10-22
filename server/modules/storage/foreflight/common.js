const path = require('path')
const sgit = require('simple-git')
const fs = require('fs-extra')
const _ = require('lodash')
const stream = require('stream')
const Promise = require('bluebird')
const pipeline = Promise.promisify(stream.pipeline)
const klaw = require('klaw')

const pageHelper = require('../../../helpers/page')
const assetHelper = require('../../../helpers/asset')
const commonDisk = require('../disk/common')
const https = require('https')

/* global WIKI */

module.exports = class FFStorageSyncModule {
  constructor() {
    // In your module's configuration
    this.git = sgit.simpleGit()
    this.repoPath = path.resolve(WIKI.ROOTPATH, WIKI.config.dataPath, 'repo')
  }

  async activated() {
    // not used
  }

  async deactivated() {
    // not used
  }

  async init() {
    try {
      WIKI.logger.info('(STORAGE/GIT) Initializing...')
      // Step 1: Ensure the repoPath directory exists
      this.repoPath = path.resolve(WIKI.ROOTPATH, this.config.localRepoPath)
      await fs.ensureDir(this.repoPath)
      // Step 2: Initialize Git with custom settings if necessary
      this.git = sgit(this.repoPath, { maxConcurrentProcesses: 1 })
      // Set custom binary path if provided
      if (!_.isEmpty(this.config.gitBinaryPath)) {
        this.git.customBinary(this.config.gitBinaryPath)
      }
      // Step 3: Check if the repository is initialized
      WIKI.logger.info('(STORAGE/GIT) Checking repository state...')
      const isRepo = await this.git.checkIsRepo()
      if (!isRepo) {
        WIKI.logger.info('(STORAGE/GIT) Initializing local repository...')
        await this.git.init()
      }
      // Step 4: Set Git configurations (disable quotePath and color output)
      try {
        await this.git.raw(['config', '--local', 'core.quotepath', 'false'])
        await this.git.raw(['config', '--local', 'color.ui', 'false'])
        await this.git.raw(['config', '--local', 'user.email', this.config.defaultEmail])
        await this.git.raw(['config', '--local', 'user.name', this.config.defaultName])
      } catch (error) {
        if (error.message.includes('could not lock config file')) {
          WIKI.logger.warn('Removing stale config lock file...')
          await fs.remove(path.join(this.repoPath, '.git', 'config.lock')) // Remove the lock file
          WIKI.logger.info('Retrying Git configuration...')
          // Retry the Git configuration after removing the lock
          await this.git.raw(['config', '--local', 'core.quotepath', 'false'])
          await this.git.raw(['config', '--local', 'color.ui', 'false'])
          await this.git.raw(['config', '--local', 'user.email', this.config.defaultEmail])
          await this.git.raw(['config', '--local', 'user.name', this.config.defaultName])
        } else {
          throw error
        }
      }

      // Step 5: Handle remotes - Remove existing ones and re-add origin
      WIKI.logger.info('(STORAGE/GIT) Checking and updating remotes...')
      const remotes = await this.git.getRemotes()
      if (remotes.length > 0) {
        WIKI.logger.info('(STORAGE/GIT) Removing existing remotes...')
        for (let remote of remotes) {
          await this.git.removeRemote(remote.name)
        }
      }

      // Add the origin remote
      let originUrl = ''
      if (_.startsWith(this.config.repoUrl, 'http')) {
        originUrl = this.config.repoUrl.replace('://', `://${encodeURIComponent(this.config.basicUsername)}:${encodeURIComponent(this.config.basicPassword)}@`)
      } else {
        originUrl = `https://${encodeURIComponent(this.config.basicUsername)}:${encodeURIComponent(this.config.basicPassword)}@${this.config.repoUrl}`
      }

      WIKI.logger.info('(STORAGE/GIT) Adding origin remote...')
      await this.git.addRemote('origin', originUrl)

      // Step 6: Fetch and update remotes
      WIKI.logger.info('(STORAGE/GIT) Fetching updates from remote...')
      await this.git.fetch('origin')

      // Step 7: Check if the desired branch exists
      const branches = await this.git.branch()
      if (!_.includes(branches.all, this.config.branch) && !_.includes(branches.all, `remotes/origin/${this.config.branch}`)) {
        throw new Error(`Invalid branch! Branch ${this.config.branch} does not exist on the remote.`)
      }

      // Step 8: Checkout the specified branch
      WIKI.logger.info(`(STORAGE/GIT) Checking out branch '${this.config.branch}'...`)
      await this.git.checkout(this.config.branch)

      // Step 9: Perform initial sync
      await this.sync()

      WIKI.logger.info('(STORAGE/GIT) Initialization completed successfully.')
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Initialization failed: ${error.message}`)
      throw error
    }
  }

  async sync() {
    try {
      WIKI.logger.info('(STORAGE/GIT) Starting synchronization process...')

      // 1. Get all pages from the database
      const storagePages = await WIKI.models.pages.query()

      // 2. Get active branches from open PRs
      const activeBranches = await this.getActiveBranches()
      const branchSet = new Set(activeBranches)

      // 3. Create a mapping of files to the active branches they exist in
      const fileBranchMap = {} // { fileName: Set(branches) }

      for (const branch of branchSet) {
        // Fetch the branch if it doesn't exist locally
        const localBranches = (await this.git.branchLocal()).all
        if (!localBranches.includes(branch)) {
          WIKI.logger.info(`(STORAGE/GIT) Fetching branch '${branch}' from remote...`)
          await this.git.fetch('origin', branch)
        }

        // Use git ls-tree to get the list of files in the branch without checking it out
        const gitFilesOutput = await this.git.raw(['ls-tree', '-r', '--name-only', `origin/${branch}`])
        const gitFiles = gitFilesOutput.split('\n').filter(file => file.length > 0)
        for (const file of gitFiles) {
          if (!fileBranchMap[file]) {
            fileBranchMap[file] = new Set()
          }
          fileBranchMap[file].add(branch)
        }
      }

      // 4. Build a map of storage pages for easy access
      const storagePagesMap = {} // { fileName: page }
      for (const page of storagePages) {
        let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`
        if (WIKI.config.lang.namespacing && WIKI.config.lang.code !== page.localeCode) {
          fileName = `${page.localeCode}/${fileName}`
        }
        storagePagesMap[fileName] = page
      }

      // 5. Process each file in the Git repository
      const filesInGit = new Set()
      for (const [fileName, branches] of Object.entries(fileBranchMap)) {
        filesInGit.add(fileName)
        const inMain = branches.has(this.config.branch)
        const pageExists = storagePagesMap[fileName]

        // Skip processing if the file is not a page (e.g., skip assets or other files)
        const contentType = pageHelper.getContentType(fileName)
        if (!contentType) {
          continue
        }

        // Read the file content directly from the Git repository
        const branchToUse = [...branches][0] // Use any active branch the file exists in
        const gitFilePath = fileName

        let content
        try {
          content = await this.git.show([`origin/${branchToUse}:${gitFilePath}`])
        } catch (error) {
          WIKI.logger.error(`(STORAGE/GIT) Failed to read file '${gitFilePath}' from branch '${branchToUse}': ${error.message}`)
          continue
        }

        if (pageExists) {
          // Page exists in the database
          if (inMain && !pageExists.isPublished) {
            // Publish the page if it's in the main branch and not already published
            await WIKI.models.pages.query().patch({ isPublished: true }).where('id', pageExists.id)
            WIKI.logger.info(`(STORAGE/GIT) Published page '${pageExists.path}'`)
          } else if (!inMain && pageExists.isPublished) {
            // Unpublish the page if it's not in the main branch but is published
            await WIKI.models.pages.query().patch({ isPublished: false }).where('id', pageExists.id)
            WIKI.logger.info(`(STORAGE/GIT) Unpublished page '${pageExists.path}'`)
          }
          // Optionally update the content if needed
          // For example, you could compare and update the content here
        } else {
          // Page does not exist in the database, so process it
          WIKI.logger.info(`(STORAGE/GIT) Adding new page '${fileName}' to the database...`)

          // Use commonDisk.processPage to handle the page content
          // Since processPage expects a file on disk, write to a temporary file
          const tempDir = path.join(this.repoPath, 'temp')
          await fs.ensureDir(tempDir)
          const tempFilePath = path.join(tempDir, fileName)
          await fs.outputFile(tempFilePath, content, 'utf8')

          try {
            await commonDisk.processPage({
              user: await WIKI.models.users.getRootUser(),
              relPath: fileName,
              fullPath: tempDir,
              contentType: contentType,
              moduleName: 'GIT',
              isPublished: inMain // Set isPublished based on whether it's in main branch
            })
            WIKI.logger.info(`(STORAGE/GIT) Added page '${fileName}' to the database`)
          } catch (err) {
            WIKI.logger.warn(`(STORAGE/GIT) Failed to process and add page '${fileName}': ${err.message}`)
          } finally {
            // Clean up temporary file
            await fs.unlink(tempFilePath)
          }
        }
      }

      // 6. Identify pages to delete (files not present in any active branch)
      const filesInDB = new Set(Object.keys(storagePagesMap))
      const filesDeletedFromAllBranches = [...filesInDB].filter(file => !filesInGit.has(file))

      const pagesToDelete = filesDeletedFromAllBranches.map(fileName => storagePagesMap[fileName])

      if (pagesToDelete.length > 0) {
        for (const page of pagesToDelete) {
          try {
            await WIKI.models.pages.deletePage({
              user: await WIKI.models.users.getRootUser(),
              path: page.path,
              locale: page.localeCode,
              skipStorage: true
            })
            WIKI.logger.info(`(STORAGE/GIT) Deleted page '${page.path}' from the database`)
          } catch (error) {
            WIKI.logger.error(`(STORAGE/GIT) Failed to delete page '${page.path}': ${error.message}`)
          }
        }
        WIKI.logger.info(`(STORAGE/GIT) Deleted ${pagesToDelete.length} pages no longer present in any active Git branch.`)
      }

      // 7. Unpublish pages not in main branch
      const pagesToUnpublish = []
      for (const [fileName, branches] of Object.entries(fileBranchMap)) {
        const inMain = branches.has(this.config.branch)
        if (!inMain) {
          const page = storagePagesMap[fileName]
          if (page && page.isPublished) {
            pagesToUnpublish.push(page)
          }
        }
      }

      if (pagesToUnpublish.length > 0) {
        await WIKI.models.pages.query()
          .patch({ isPublished: false })
          .whereIn('id', pagesToUnpublish.map(page => page.id))
        WIKI.logger.info(`(STORAGE/GIT) Unpublished ${pagesToUnpublish.length} pages not in '${this.config.branch}' branch.`)
      }

      // 8. Publish pages in main branch
      const pagesToPublish = []
      for (const [fileName, branches] of Object.entries(fileBranchMap)) {
        const inMain = branches.has(this.config.branch)
        if (inMain) {
          const page = storagePagesMap[fileName]
          if (page && !page.isPublished) {
            pagesToPublish.push(page)
          }
        }
      }

      if (pagesToPublish.length > 0) {
        await WIKI.models.pages.query()
          .patch({ isPublished: true })
          .whereIn('id', pagesToPublish.map(page => page.id))
        WIKI.logger.info(`(STORAGE/GIT) Published ${pagesToPublish.length} pages in '${this.config.branch}' branch.`)
      }

      // 9. Reset to the main branch
      await this.git.checkout(this.config.branch)

      // 10. Clean up temporary directory
      await fs.remove(path.join(this.repoPath, 'temp'))

      WIKI.logger.info('(STORAGE/GIT) Synchronization completed successfully.')
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Synchronization failed: ${error.message}`)
      throw error
    }
  }

  async getActiveBranches() {
    return new Promise((resolve, reject) => {
      try {
        // Extract repository owner and name from the repo URL
        const repoUrl = this.config.repoUrl.replace(/\.git$/, '')
        const repoMatch = repoUrl.match(/[:/]([^/]+)\/([^/]+)$/)
        if (!repoMatch) {
          throw new Error('Invalid repository URL format.')
        }
        const owner = repoMatch[1]
        const repo = repoMatch[2]

        // GitHub API URL for listing open PRs
        const apiPath = `/repos/${owner}/${repo}/pulls?state=open&per_page=100`

        const options = {
          hostname: 'api.github.com',
          path: apiPath,
          method: 'GET',
          headers: {
            'User-Agent': 'WikiJS-Storage-GitModule', // Custom User-Agent
            'Accept': 'application/vnd.github.v3+json',
            // Include authorization header with the GitHub token
            'Authorization': `token ${this.config.basicPassword}`
          }
        }

        const req = https.request(options, (res) => {
          let data = ''

          res.on('data', (chunk) => {
            data += chunk
          })

          res.on('end', () => {
            if (res.statusCode >= 200 && res.statusCode < 300) {
              let pullRequests
              try {
                pullRequests = JSON.parse(data)
              } catch (parseError) {
                WIKI.logger.error(`(STORAGE/GIT) Failed to parse GitHub API response: ${parseError.message}`)
                return reject(parseError)
              }

              // Extract branch names from open PRs
              const activeBranches = pullRequests.map(pr => pr.head.ref)

              // Always include the main branch
              if (!activeBranches.includes(this.config.branch)) {
                activeBranches.push(this.config.branch)
              }

              WIKI.logger.info(`(STORAGE/GIT) Active branches: ${activeBranches.join(', ')}`)

              resolve(activeBranches)
            } else {
              reject(new Error(`GitHub API request failed with status code ${res.statusCode}: ${res.statusMessage}`))
            }
          })
        })

        req.on('error', (error) => {
          reject(new Error(`GitHub API request error: ${error.message}`))
        })

        req.end()
      } catch (error) {
        reject(error)
      }
    }).catch((error) => {
      WIKI.logger.error(`(STORAGE/GIT) Failed to get active branches: ${error.message}`)
      // Fallback to main branch if failed
      return [this.config.branch]
    })
  }

  /**
   * Closes the Pull Request associated with the given branch.
   * @param {string} branchName - The name of the branch.
   */
  async closePR(branchName) {
    try {
      // Extract repository owner and name from the repo URL
      const repoUrl = this.config.repoUrl.replace(/\.git$/, '')
      const repoMatch = repoUrl.match(/[:/]([^/]+)\/([^/]+)$/)
      if (!repoMatch) {
        throw new Error('Invalid repository URL format.')
      }
      const owner = repoMatch[1]
      const repo = repoMatch[2]

      // Fetch open PRs to find the one associated with the branch
      const apiPath = `/repos/${owner}/${repo}/pulls?state=open&head=${owner}:${branchName}`

      const options = {
        hostname: 'api.github.com',
        path: apiPath,
        method: 'GET',
        headers: {
          'User-Agent': 'WikiJS-Storage-GitModule', // Custom User-Agent
          'Accept': 'application/vnd.github.v3+json',
          'Authorization': `token ${this.config.basicPassword}`
        }
      }

      const activePRs = await this.makeGitHubRequest(options)

      if (activePRs.length === 0) {
        WIKI.logger.info(`(STORAGE/GIT) No open PR found for branch '${branchName}'.`)
        return
      }

      for (const pr of activePRs) {
        // Close the PR
        const closePath = `/repos/${owner}/${repo}/pulls/${pr.number}`

        const closeOptions = {
          hostname: 'api.github.com',
          path: closePath,
          method: 'PATCH',
          headers: {
            'User-Agent': 'WikiJS-Storage-GitModule',
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': `token ${this.config.basicPassword}`,
            'Content-Type': 'application/json'
          }
        }

        const closeData = JSON.stringify({ state: 'closed' })

        await this.makeGitHubRequest(closeOptions, closeData)

        WIKI.logger.info(`(STORAGE/GIT) Closed PR #${pr.number} associated with branch '${branchName}'.`)
      }
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Failed to close PR for branch '${branchName}': ${error.message}`)
    }
  }

  /**
   * Makes a GitHub API request.
   * @param {object} options - HTTPS request options.
   * @param {string} [postData] - Data to send with the request (for PATCH requests).
   * @returns {Promise<any>} - Parsed JSON response.
   */
  makeGitHubRequest(options, postData = null) {
    return new Promise((resolve, reject) => {
      const req = https.request(options, (res) => {
        let data = ''

        res.on('data', (chunk) => {
          data += chunk
        })

        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            try {
              const parsedData = JSON.parse(data)
              resolve(parsedData)
            } catch (parseError) {
              WIKI.logger.error(`(STORAGE/GIT) Failed to parse GitHub API response: ${parseError.message}`)
              reject(parseError)
            }
          } else {
            WIKI.logger.error(`(STORAGE/GIT) GitHub API request failed with status code ${res.statusCode}: ${res.statusMessage}`)
            reject(new Error(`GitHub API request failed with status code ${res.statusCode}: ${res.statusMessage}`))
          }
        })
      })

      req.on('error', (error) => {
        WIKI.logger.error(`(STORAGE/GIT) GitHub API request error: ${error.message}`)
        reject(error)
      })

      if (postData) {
        req.write(postData)
      }

      req.end()
    })
  }

  async processFiles(files, user) {
    for (const item of files) {
      const contentType = pageHelper.getContentType(item.relPath)
      const fileExists = await fs.pathExists(item.file.path)
      if (!item.binary && contentType) {
        // -> Page

        if (fileExists && !item.importAll && item.relPath !== item.oldPath) {
          // Page was renamed by git, so rename in DB
          WIKI.logger.info(`(STORAGE/GIT) Page marked as renamed: from ${item.oldPath} to ${item.relPath}`)

          const contentPath = pageHelper.getPagePath(item.oldPath)
          const contentDestinationPath = pageHelper.getPagePath(item.relPath)
          await WIKI.models.pages.movePage({
            user: user,
            path: contentPath.path,
            destinationPath: contentDestinationPath.path,
            locale: contentPath.locale,
            destinationLocale: contentPath.locale,
            skipStorage: true
          })
        } else if (!fileExists && !item.importAll && item.deletions > 0 && item.insertions === 0) {
          // Page was deleted by git, can safely mark as deleted in DB
          WIKI.logger.info(`(STORAGE/GIT) Page marked as deleted: ${item.relPath}`)

          const contentPath = pageHelper.getPagePath(item.relPath)
          await WIKI.models.pages.deletePage({
            user: user,
            path: contentPath.path,
            locale: contentPath.locale,
            skipStorage: true
          })
          continue
        }

        try {
          await commonDisk.processPage({
            user,
            relPath: item.relPath,
            fullPath: this.repoPath,
            contentType: contentType,
            moduleName: 'GIT'
          })
          WIKI.logger.info(`(STORAGE/GIT) Successfully processed page '${item.relPath}'.`)
        } catch (err) {
          WIKI.logger.warn(`(STORAGE/GIT) Failed to process and add page '${item.relPath}': ${err.message}`)
        }
      } else {
        // -> Asset

        if (fileExists && !item.importAll && ((item.before === item.after) || (item.deletions === 0 && item.insertions === 0))) {
          // Asset was renamed by git, so rename in DB
          WIKI.logger.info(`(STORAGE/GIT) Asset marked as renamed: from ${item.oldPath} to ${item.relPath}`)

          const fileHash = assetHelper.generateHash(item.relPath)
          const assetToRename = await WIKI.models.assets.query().findOne({ hash: fileHash })
          if (assetToRename) {
            await WIKI.models.assets.query().patch({
              filename: item.relPath,
              hash: fileHash
            }).findById(assetToRename.id)
            await assetToRename.deleteAssetCache()
            WIKI.logger.info(`(STORAGE/GIT) Successfully renamed asset '${item.relPath}'.`)
          } else {
            WIKI.logger.info(`(STORAGE/GIT) Asset was not found in the DB, nothing to rename: ${item.relPath}`)
          }
          continue
        } else if (!fileExists && !item.importAll && ((item.before > 0 && item.after === 0) || (item.deletions > 0 && item.insertions === 0))) {
          // Asset was deleted by git, can safely mark as deleted in DB
          WIKI.logger.info(`(STORAGE/GIT) Asset marked as deleted: ${item.relPath}`)

          const fileHash = assetHelper.generateHash(item.relPath)
          const assetToDelete = await WIKI.models.assets.query().findOne({ hash: fileHash })
          if (assetToDelete) {
            await WIKI.models.knex('assetData').where('id', assetToDelete.id).del()
            await WIKI.models.assets.query().deleteById(assetToDelete.id)
            await assetToDelete.deleteAssetCache()
            WIKI.logger.info(`(STORAGE/GIT) Successfully deleted asset '${item.relPath}'.`)
          } else {
            WIKI.logger.info(`(STORAGE/GIT) Asset was not found in the DB, nothing to delete: ${item.relPath}`)
          }
          continue
        }

        try {
          await commonDisk.processAsset({
            user,
            relPath: item.relPath,
            file: item.file,
            contentType: contentType,
            moduleName: 'GIT'
          })
          WIKI.logger.info(`(STORAGE/GIT) Successfully processed asset '${item.relPath}'.`)
        } catch (err) {
          WIKI.logger.warn(`(STORAGE/GIT) Failed to process and add asset '${item.relPath}': ${err.message}`)
        }
      }
    }
  }

  async created(page) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Committing new file [${page.localeCode}] ${page.path}...`)
      let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`

      // Generate branch name for this new page
      const branchName = `create/new-${page.path.replace(/\//g, '-')}`

      // Check if the branch exists locally or remotely
      const branches = await this.git.branch(['-a'])
      const branchExists = _.includes(branches.all, `remotes/origin/${branchName}`) || _.includes(branches.all, branchName)

      if (branchExists) {
        WIKI.logger.info(`(STORAGE/GIT) Branch '${branchName}' already exists. Deleting it.`)
        await this.git.raw(['branch', '-D', branchName])
      }

      WIKI.logger.info(`(STORAGE/GIT) Creating and switching to new branch: '${branchName}'`)
      await this.git.checkoutLocalBranch(branchName)

      if (WIKI.config.lang.namespacing && WIKI.config.lang.code !== page.localeCode) {
        fileName = `${page.localeCode}/${fileName}`
      }
      const filePath = path.join(this.repoPath, fileName)
      await fs.outputFile(filePath, page.injectMetadata(), 'utf8')

      const gitFilePath = `./${fileName}`
      if ((await this.git.checkIgnore(gitFilePath)).length === 0) {
        await this.git.add(gitFilePath)
        await this.git.commit(`docs: create ${page.path}`, fileName, {
          '--author': `"${page.authorName} <${page.authorEmail}>"`
        })
        if (!branchExists) {
          await this.git.push('origin', branchName, ['--set-upstream'])
        } else {
          await this.git.push('origin', branchName)
        }
      }

      // After creation, run sync to update the database
      await this.sync()
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Error creating page '${page.path}': ${error.message}`)
    }
  }

  async updated(page) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Committing updated file [${page.localeCode}] ${page.path}...`)
      let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`

      const branchName = `update/${page.path.replace(/\//g, '-')}`

      // Check if the branch exists locally or remotely
      const branches = await this.git.branch(['-a'])
      const branchExists = _.includes(branches.all, `remotes/origin/${branchName}`) || _.includes(branches.all, branchName)

      if (branchExists) {
        WIKI.logger.info(`(STORAGE/GIT) Branch '${branchName}' already exists. Checking it out.`)
        await this.git.checkout(branchName)
      } else {
        WIKI.logger.info(`(STORAGE/GIT) Creating and switching to new branch: '${branchName}'`)
        await this.git.checkoutLocalBranch(branchName)
      }

      if (WIKI.config.lang.namespacing && WIKI.config.lang.code !== page.localeCode) {
        fileName = `${page.localeCode}/${fileName}`
      }
      const filePath = path.join(this.repoPath, fileName)
      await fs.outputFile(filePath, page.injectMetadata(), 'utf8')

      const gitFilePath = `./${fileName}`
      if ((await this.git.checkIgnore(gitFilePath)).length === 0) {
        await this.git.add(gitFilePath)
        await this.git.commit(`docs: update ${page.path}`, fileName, {
          '--author': `"${page.authorName} <${page.authorEmail}>"`
        })

        if (!branchExists) {
          await this.git.push('origin', branchName, ['--set-upstream'])
        } else {
          await this.git.push('origin', branchName)
        }
      }

      // After update, run sync to update the database
      await this.sync()
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Error updating page '${page.path}': ${error.message}`)
    }
  }

  async deleted(page) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Deleting file [${page.localeCode}] ${page.path} from the repository...`)

      // Define the file name based on the page path and content type
      let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`
      if (WIKI.config.lang.namespacing && WIKI.config.lang.code !== page.localeCode) {
        fileName = `${page.localeCode}/${fileName}`
      }

      // Get active branches from open PRs
      const activeBranches = await this.getActiveBranches()

      for (const branch of activeBranches) {
        // Checkout the branch
        await this.git.checkout(branch)

        // Pull latest changes
        await this.git.pull('origin', branch)

        // Check if the file exists in this branch
        const gitFiles = await this.git.raw(['ls-tree', '-r', '--name-only', `origin/${branch}`])
        const fileExistsInBranch = gitFiles.split('\n').includes(fileName)

        if (fileExistsInBranch) {
          // Remove the file from the local repo path
          const filePath = path.join(this.repoPath, fileName)
          const fileExists = await fs.pathExists(filePath)
          if (!fileExists) {
            // If the file doesn't exist locally, checkout the file
            await this.git.checkout([branch, '--', fileName])
          }
          await fs.remove(filePath)

          // Stage the file deletion and commit the change
          await this.git.rm(fileName)
          await this.git.commit(`docs: delete ${page.path}`, fileName, {
            '--author': `"${page.authorName} <${page.authorEmail}>"`
          })

          // Push the deletion to the branch
          await this.git.push('origin', branch)

          WIKI.logger.info(`(STORAGE/GIT) File ${fileName} deleted from repository and changes pushed to branch ${branch}.`)

          // Close the corresponding PR if it exists
          await this.closePR(branch)
        } else {
          WIKI.logger.warn(`(STORAGE/GIT) File ${fileName} does not exist in branch ${branch}, skipping deletion for this branch.`)
        }
      }

      // After processing, checkout back to the main branch
      await this.git.checkout(this.config.branch)
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Error deleting file: ${error.message}`)
    }
  }

  async assetUploaded(asset) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Committing new file ${asset.path}...`)
      const filePath = path.join(this.repoPath, asset.path)
      await fs.outputFile(filePath, asset.data, 'utf8')

      await this.git.add(`./${asset.path}`)
      await this.git.commit(`docs: upload ${asset.path}`, asset.path, {
        '--author': `"${asset.authorName} <${asset.authorEmail}>"`
      })
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Failed to upload asset '${asset.path}': ${error.message}`)
    }
  }

  async assetDeleted(asset) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Committing removed file ${asset.path}...`)

      await this.git.rm(`./${asset.path}`)
      await this.git.commit(`docs: delete ${asset.path}`, asset.path, {
        '--author': `"${asset.authorName} <${asset.authorEmail}>"`
      })
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Failed to delete asset '${asset.path}': ${error.message}`)
    }
  }

  async assetRenamed(asset) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Committing file move from ${asset.path} to ${asset.destinationPath}...`)

      await this.git.mv(`./${asset.path}`, `./${asset.destinationPath}`)
      await this.git.commit(`docs: rename ${asset.path} to ${asset.destinationPath}`, [asset.path, asset.destinationPath], {
        '--author': `"${asset.moveAuthorName} <${asset.moveAuthorEmail}>"`
      })
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Failed to rename asset from '${asset.path}' to '${asset.destinationPath}': ${error.message}`)
    }
  }

  async getLocalLocation(asset) {
    return path.join(this.repoPath, asset.path)
  }

  async importAll() {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Importing all content from local Git repo to the DB...`)

      const rootUser = await WIKI.models.users.getRootUser()

      await pipeline(
        klaw(this.repoPath, {
          filter: (f) => {
            return !_.includes(f, '.git')
          }
        }),
        new stream.Transform({
          objectMode: true,
          transform: async (file, enc, cb) => {
            const relPath = file.path.substr(this.repoPath.length + 1)
            if (file.stats.size < 1) {
              // Skip directories and zero-byte files
              return cb()
            } else if (relPath && relPath.length > 3) {
              WIKI.logger.info(`(STORAGE/GIT) Processing ${relPath}...`)
              await this.processFiles([{
                user: rootUser,
                relPath,
                file,
                deletions: 0,
                insertions: 0,
                importAll: true
              }], rootUser)
            }
            cb()
          }
        })
      )

      commonDisk.clearFolderCache()

      WIKI.logger.info('(STORAGE/GIT) Import completed.')
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Import failed: ${error.message}`)
    }
  }

  async syncUntracked() {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Adding all untracked content...`)

      // -> Pages
      await pipeline(
        WIKI.models.knex.column('id', 'path', 'localeCode', 'title', 'description', 'contentType', 'content', 'isPublished', 'updatedAt', 'createdAt', 'editorKey').select().from('pages').where({
          isPrivate: false
        }).stream(),
        new stream.Transform({
          objectMode: true,
          transform: async (page, enc, cb) => {
            const pageObject = await WIKI.models.pages.query().findById(page.id)
            page.tags = await pageObject.$relatedQuery('tags')

            let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`
            if (WIKI.config.lang.namespacing && WIKI.config.lang.code !== page.localeCode) {
              fileName = `${page.localeCode}/${fileName}`
            }
            WIKI.logger.info(`(STORAGE/GIT) Adding page ${fileName}...`)
            const filePath = path.join(this.repoPath, fileName)
            await fs.outputFile(filePath, pageHelper.injectPageMetadata(page), 'utf8')
            await this.git.add(`./${fileName}`)
            cb()
          }
        })
      )

      // -> Assets
      const assetFolders = await WIKI.models.assetFolders.getAllPaths()

      await pipeline(
        WIKI.models.knex.column('filename', 'folderId', 'data').select().from('assets').join('assetData', 'assets.id', '=', 'assetData.id').stream(),
        new stream.Transform({
          objectMode: true,
          transform: async (asset, enc, cb) => {
            const filename = (asset.folderId && asset.folderId > 0) ? `${_.get(assetFolders, asset.folderId)}/${asset.filename}` : asset.filename
            WIKI.logger.info(`(STORAGE/GIT) Adding asset ${filename}...`)
            await fs.outputFile(path.join(this.repoPath, filename), asset.data)
            await this.git.add(`./${filename}`)
            cb()
          }
        })
      )

      await this.git.commit(`docs: add all untracked content`)
      WIKI.logger.info('(STORAGE/GIT) All content is now tracked.')
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Failed to sync untracked content: ${error.message}`)
    }
  }

  async purge() {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Purging local repository...`)
      await fs.emptyDir(this.repoPath)
      WIKI.logger.info('(STORAGE/GIT) Local repository is now empty. Reinitializing...')
      await this.init()
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Failed to purge repository: ${error.message}`)
    }
  }
}

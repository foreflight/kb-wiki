const path = require('path')
const sgit = require('simple-git')
const fs = require('fs-extra')
const _ = require('lodash')
const stream = require('stream')
const Promise = require('bluebird')
const pipeline = Promise.promisify(stream.pipeline)
const klaw = require('klaw')
const Page = require('../../../models/pages')

const pageHelper = require('../../../helpers/page')
const assetHelper = require('../../../helpers/asset')
const commonDisk = require('../disk/common')

/* global WIKI */
module.exports = class FFStorageSyncModule {
  constructor () {
    this.git = sgit.simpleGit()
    this.repoPath = path.resolve(WIKI.ROOTPATH, WIKI.config.dataPath, 'repo')
  }

  async activated () {
    console.log('activated')
    console.log(WIKI)
  }

  async deactivated () {
    console.log('deactivated')
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
        originUrl = this.config.repoUrl.replace('://', `://${encodeURI(this.config.basicUsername)}:${encodeURI(this.config.basicPassword)}@`)
      } else {
        originUrl = `https://${encodeURI(this.config.basicUsername)}:${encodeURI(this.config.basicPassword)}@${this.config.repoUrl}`
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
    // 1. Get all pages on the local disk (from the database)
    const storagePages = await WIKI.models.pages.query()

    // 2. Get all pages from the Git `main` branch
    const gitPagesRequest = await this.git.raw(['ls-tree', '-r', '--name-only', 'origin/main'])
    const gitPages = gitPagesRequest.split('\n').filter(file => file.length > 0)

    // 3. Identify pages that are in the local storage but not in the Git main branch
    const pagesToUnpublish = storagePages.filter(page => {
      const fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`

      return !gitPages.includes(fileName)
    })

    // 4. Identify pages that ARE in the Git main branch and need to be published
    const pagesToPublish = storagePages.filter(page => {
      const fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`
      return gitPages.includes(fileName)
    })

    // 5. Unpublish pages that are not in the Git main branch
    if (pagesToUnpublish.length > 0) {
      await WIKI.models.pages.query().patch({ isPublished: false }).whereIn('id', pagesToUnpublish.map(page => page.id))
      console.log('Unpublished Pages:', pagesToUnpublish)
    }

    // 6. Publish pages that are in the Git main branch
    if (pagesToPublish.length > 0) {
      await WIKI.models.pages.query().patch({ isPublished: true }).whereIn('id', pagesToPublish.map(page => page.id))
      console.log('Published Pages:', pagesToPublish)
    }
    Page.flushCache()
    const currentCommitLog = _.get(await this.git.log(['-n', '1', this.config.branch, '--']), 'latest', {})

    const rootUser = await WIKI.models.users.getRootUser()

    // Pull rebase
    if (_.includes(['sync', 'pull'], this.mode)) {
      WIKI.logger.info(`(STORAGE/GIT) Performing pull rebase from origin on branch ${this.config.branch}...`)
      await this.git.pull('origin', this.config.branch, ['--rebase'])
    }

    // Push
    if (_.includes(['sync', 'push'], this.mode)) {
      WIKI.logger.info(`(STORAGE/GIT) Performing push to origin on branch ${this.config.branch}...`)
      let pushOpts = ['--signed=if-asked']
      if (this.mode === 'push') {
        pushOpts.push('--force')
      }
      await this.git.push('origin', this.config.branch, pushOpts)
    }

    // Process Changes
    if (_.includes(['sync', 'pull'], this.mode)) {
      const latestCommitLog = _.get(await this.git.log(['-n', '1', this.config.branch, '--']), 'latest', {})

      const diff = await this.git.diffSummary(['-M', currentCommitLog.hash, latestCommitLog.hash])
      if (_.get(diff, 'files', []).length > 0) {
        let filesToProcess = []
        const filePattern = /(.*?)(?:{(.*?))? => (?:(.*?)})?(.*)/
        for (const f of diff.files) {
          const fMatch = f.file.match(filePattern)
          const fNames = {
            old: null,
            new: null
          }
          if (!fMatch) {
            fNames.old = f.file
            fNames.new = f.file
          } else if (!fMatch[2] && !fMatch[3]) {
            fNames.old = fMatch[1]
            fNames.new = fMatch[4]
          } else {
            fNames.old = (fMatch[1] + fMatch[2] + fMatch[4]).replace('//', '/')
            fNames.new = (fMatch[1] + fMatch[3] + fMatch[4]).replace('//', '/')
          }
          const fPath = path.join(this.repoPath, fNames.new)
          let fStats = { size: 0 }
          try {
            fStats = await fs.stat(fPath)
          } catch (err) {
            if (err.code !== 'ENOENT') {
              WIKI.logger.warn(`(STORAGE/GIT) Failed to access file ${f.file}! Skipping...`)
              continue
            }
          }

          filesToProcess.push({
            ...f,
            file: {
              path: fPath,
              stats: fStats
            },
            oldPath: fNames.old,
            relPath: fNames.new
          })
        }
        await this.processFiles(filesToProcess, rootUser)
      }
    }
  }
  /**
   * Process Files
   *
   * @param {Array<String>} files Array of files to process
   */
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
        } catch (err) {
          WIKI.logger.warn(`(STORAGE/GIT) Failed to process ${item.relPath}`)
          WIKI.logger.warn(err)
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
        } catch (err) {
          WIKI.logger.warn(`(STORAGE/GIT) Failed to process asset ${item.relPath}`)
          WIKI.logger.warn(err)
        }
      }
    }
  }
  /**
   * CREATE
   *
   * @param {Object} page Page to create
   */
  async created(page) {
    WIKI.logger.info(`(STORAGE/GIT) Committing new file [${page.localeCode}] ${page.path}...`)
    let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`

    // Generate branch name for this new page
    const branchName = `create/new-${page.path.replace(/\//g, '-')}`

    // Check if the branch exists locally or remotely
    const branches = await this.git.branch(['-a'])
    const branchExists = _.includes(branches.all, `remotes/origin/${branchName}`) || _.includes(branches.all, branchName)

    if (branchExists) {
      WIKI.logger.info(`(STORAGE/GIT) Branch ${branchName} already exists. Deleting it.`)
      await this.git.raw(['branch', '-D', branchName])
    }

    WIKI.logger.info(`(STORAGE/GIT) Creating and switching to new branch: ${branchName}`)
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
    await this.git.checkout('main')
    this.sync()
  }
  /**
   * UPDATE
   *
   * @param {Object} page Page to update
   */
  async updated(page) {
    WIKI.logger.info(`(STORAGE/GIT) Committing updated file [${page.localeCode}] ${page.path}...`)
    let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`

    const branchName = `create/new-${page.path.replace(/\//g, '-')}`

    // Check if the branch exists locally or remotely
    const branches = await this.git.branch(['-a'])
    const branchExists = _.includes(branches.all, `remotes/origin/${branchName}`) || _.includes(branches.all, branchName)

    if (branchExists) {
      WIKI.logger.info(`(STORAGE/GIT) Branch ${branchName} already exists. Checking it out.`)
      await this.git.checkout(branchName)
    } else {
      WIKI.logger.info(`(STORAGE/GIT) Creating and switching to new branch: ${branchName}`)
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
    await this.git.checkout('main')

    this.sync()
  }
  /**
 * DELETE
 *
 * @param {Object} page Page to delete
 */
  async deleted(page) {
    try {
      WIKI.logger.info(`(STORAGE/GIT) Checking for file [${page.localeCode}] ${page.path} in the remote repository...`)

      // Define the file name based on the page path and content type
      let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`

      // Use `git ls-tree` to check if the file exists in the remote `main` branch
      const gitFiles = await this.git.raw(['ls-tree', '-r', '--name-only', 'origin/main'])
      const fileExistsInRemote = gitFiles.split('\n').includes(fileName)

      // If the file exists in the remote repository, delete it and push the change
      if (fileExistsInRemote) {
        // Remove the file from the local repo path
        await fs.remove(path.join(this.repoPath, fileName))

        // Stage the file deletion and commit the change
        await this.git.rm(fileName)
        await this.git.commit(`docs: delete ${page.path}`, fileName, {
          '--author': `"${page.authorName} <${page.authorEmail}>"`
        })

        // Push the deletion directly to the main branch
        await this.git.push('origin', 'main')

        WIKI.logger.info(`(STORAGE/GIT) File ${fileName} deleted from remote repository and changes pushed to main branch.`)
      } else {
        WIKI.logger.warn(`(STORAGE/GIT) File ${fileName} does not exist in the remote repository, skipping deletion.`)
      }
    } catch (error) {
      WIKI.logger.error(`(STORAGE/GIT) Error deleting file: ${error.message}`)
    }
  }
  /**
   * ASSET UPLOAD
   *
   * @param {Object} asset Asset to upload
   */
  async assetUploaded (asset) {
    WIKI.logger.info(`(STORAGE/GIT) Committing new file ${asset.path}...`)
    const filePath = path.join(this.repoPath, asset.path)
    await fs.outputFile(filePath, asset.data, 'utf8')

    await this.git.add(`./${asset.path}`)
    await this.git.commit(`docs: upload ${asset.path}`, asset.path, {
      '--author': `"${asset.authorName} <${asset.authorEmail}>"`
    })
  }
  /**
   * ASSET DELETE
   *
   * @param {Object} asset Asset to upload
   */
  async assetDeleted (asset) {
    WIKI.logger.info(`(STORAGE/GIT) Committing removed file ${asset.path}...`)

    await this.git.rm(`./${asset.path}`)
    await this.git.commit(`docs: delete ${asset.path}`, asset.path, {
      '--author': `"${asset.authorName} <${asset.authorEmail}>"`
    })
  }
  /**
   * ASSET RENAME
   *
   * @param {Object} asset Asset to upload
   */
  async assetRenamed (asset) {
    WIKI.logger.info(`(STORAGE/GIT) Committing file move from ${asset.path} to ${asset.destinationPath}...`)

    await this.git.mv(`./${asset.path}`, `./${asset.destinationPath}`)
    await this.git.commit(`docs: rename ${asset.path} to ${asset.destinationPath}`, [asset.path, asset.destinationPath], {
      '--author': `"${asset.moveAuthorName} <${asset.moveAuthorEmail}>"`
    })
  }
  async getLocalLocation (asset) {
    return path.join(this.repoPath, asset.path)
  }
  /**
   * HANDLERS
   */
  async importAll() {
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
  }
  async syncUntracked() {
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
  }
  async purge() {
    WIKI.logger.info(`(STORAGE/GIT) Purging local repository...`)
    await fs.emptyDir(this.repoPath)
    WIKI.logger.info('(STORAGE/GIT) Local repository is now empty. Reinitializing...')
    await this.init()
  }
}

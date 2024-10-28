const path = require('path')
const sgit = require('simple-git')
const fs = require('fs-extra')
const Page = require('../../../models/pages')

const pageHelper = require('../../../helpers/page')
const commonDisk = require('../disk/common')

/* global WIKI */

module.exports = class FFStorageSyncModule {
  constructor () {
    this.git = sgit.simpleGit()
    this.repoPath = path.resolve(WIKI.ROOTPATH, WIKI.config.dataPath, 'repo')
    this.rootUser = null
  }

  async activated () {}

  async deactivated () {}

  async init() {
    try {
      this.rootUser = await WIKI.models.users.getRootUser()
      await fs.emptyDir(this.repoPath)
      await fs.ensureDir(this.repoPath)
      let originUrl = this.config.repoUrl.replace('://', `://${encodeURIComponent(this.config.basicUsername)}:${encodeURIComponent(this.config.basicPassword)}@`)
      await sgit().clone(originUrl, this.repoPath)
      this.git = sgit(this.repoPath, { maxConcurrentProcesses: 1 })
      await this.git.raw(['config', '--local', 'core.quotepath', 'false'])
      await this.git.raw(['config', '--local', 'color.ui', 'false'])
      await this.git.raw(['config', '--local', 'user.email', this.config.defaultEmail])
      await this.git.raw(['config', '--local', 'user.name', this.config.defaultName])
      await this.git.checkout(this.config.branch)
      await this.deleteDBPages()
      await this.proccessBranches()
    } catch (error) {
      WIKI.logger.error('(STORAGE/GIT) Error initializing module')
      throw error
    }
  }
  async deleteDBPages() {
    try {
      const pages = await WIKI.models.pages.query().where({ })
      for (const page of Object.values(pages)) {
        const opts = {
          id: page.id,
          user: this.rootUser,
          skipStorage: false
        }
        try {
          Page.deletePage(opts)
        } catch (e) {
          WIKI.logger.error('(STORAGE/GIT) Error deleting page')
        }
      }
    } catch (error) {
      WIKI.logger.error('(STORAGE/GIT) Error deleting pages')
      throw error
    }
  }

  async addNewPageFromGitToDB(pagesFilepaths, branch) {
    try {
      for (const filepath of pagesFilepaths) {
        const localFilePath = path.resolve(this.repoPath, filepath)
        const isFile = await fs.pathExists(localFilePath)
        const shouldIgnore = filepath.startsWith('.')
        if (isFile && !shouldIgnore) {
          const contentType = pageHelper.getContentType(localFilePath)
          const pageData = {
            user: this.rootUser,
            fullPath: this.repoPath,
            relPath: filepath,
            contentType: contentType,
            moduleName: 'foreflight'
          }
          await commonDisk.processPage(pageData)
          const page = await WIKI.models.pages.query().findOne({ path: filepath.replace('.md', '') })
          const isPublished = branch === this.config.branch
          await WIKI.models.pages.query().patch({ isPublished: isPublished }).findById(page.id)
        }
      }
    } catch (error) {
      WIKI.logger.error('(STORAGE/GIT) Error adding new pages to DB')
      throw error
    }
  }

  async proccessBranches() {
    try {
      await this.git.fetch('origin')
      await this.git.checkout('main')
      await this.git.pull('origin', 'main')
      const branchesRequest = await this.git.branch(['-a'])
      const branches = branchesRequest.all.filter(branch => branch !== 'main' && branch !== 'remotes/origin/main')
      for (const branch of branches) {
        if (branch !== this.config.branch && !branch.includes('HEAD')) {
          const sanitizedBranch = branch.replace('remotes/origin/', '')
          await this.git.checkout(sanitizedBranch)
          await this.git.pull('origin', sanitizedBranch)
          const gitPagesRequest = await this.git.raw([
            'diff',
            '--name-only',
            'main',
            sanitizedBranch
          ])
          const gitPages = gitPagesRequest.split('\n').filter(file => file.length > 0)
          await this.addNewPageFromGitToDB(gitPages, sanitizedBranch)
        }
      }
      await this.git.checkout(this.config.branch)
      await this.git.pull('origin', this.config.branch)
      const gitPagesRequest = await this.git.raw(['ls-tree', '-r', '--name-only', this.config.branch])
      const gitPages = gitPagesRequest.split('\n').filter(file => file.length > 0)
      await this.addNewPageFromGitToDB(gitPages, this.config.branch)
    } catch (error) {
      WIKI.logger.error('(STORAGE/GIT) Error processing branches')
      throw error
    }
  }

  async sync() {
    this.init()
  }

  async created(page) {
    try {
      let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`
      if (WIKI.config.lang.namespacing && WIKI.config.lang.code !== page.localeCode) {
        fileName = `${page.localeCode}/${fileName}`
      }
      const filePath = path.join(this.repoPath, fileName)
      await fs.outputFile(filePath, page.injectMetadata(), 'utf8')
      const sanitizedBranchName = `create-${page.path.replace(/[^\w.-]/g, '-').toLowerCase()}`
      await this.git.fetch()
      const branches = await this.git.branch(['-a'])
      const branchExists = branches.all.includes(`remotes/origin/${sanitizedBranchName}`)

      if (branchExists) {
        await this.git.checkout(sanitizedBranchName)
        await this.git.pull('origin', sanitizedBranchName)
      } else {
        await this.git.checkout(this.config.branch)
        await this.git.pull('origin', this.config.branch)
        await this.git.checkoutBranch(sanitizedBranchName, this.config.branch)
      }
      const gitFilePath = fileName
      await this.git.add(gitFilePath)
      await this.git.commit(`docs: create ${page.path}`, [gitFilePath], {
        '--author': `${page.authorName} <${page.authorEmail}>`
      })
      await this.git.push(['-u', 'origin', sanitizedBranchName])
      await this.git.checkout(this.config.branch)
      this.sync()
    } catch (error) {
      WIKI.logger.error('(STORAGE/GIT) Error creating branch:', error)
      throw error
    }
  }

  async updated(page) {
    try {
      let fileName = `${page.path}.${pageHelper.getFileExtension(page.contentType)}`
      const filePath = path.join(this.repoPath, fileName)
      const sanitizedBranchName = `create-${page.path.replace(/[^\w.-]/g, '-').toLowerCase()}`
      await this.git.fetch()
      const branches = await this.git.branch(['-a'])
      const branchExists = branches.all.includes(`remotes/origin/${sanitizedBranchName}`)

      if (page.isPublished) {
        if (branchExists) {
          await this.git.checkout(sanitizedBranchName)
          await this.git.pull('origin', sanitizedBranchName)
        } else {
          await this.git.checkout(this.config.branch)
          await this.git.pull('origin', this.config.branch)
          await this.git.checkoutBranch(sanitizedBranchName, this.config.branch)
        }
        await fs.outputFile(filePath, page.injectMetadata(), 'utf8')
        await this.git.add(fileName)
        await this.git.commit(`docs: update ${page.path}`, [fileName], {'--author': `${page.authorName} <${page.authorEmail}>`})
        await this.git.push(['-u', 'origin', sanitizedBranchName])
        await WIKI.models.pages.query().patch({ isPublished: false }).findById(page.id)
      } else {
        if (branchExists) {
          await this.git.checkout(sanitizedBranchName)
          await this.git.pull('origin', sanitizedBranchName)
        } else {
          throw new Error(`(STORAGE/GIT) Branch ${sanitizedBranchName} does not exist for unpublished page ${page.path}`)
        }
        await fs.outputFile(filePath, page.injectMetadata(), 'utf8')
        await this.git.add(fileName)
        await this.git.commit(`docs: update ${page.path}`, [fileName], {'--author': `${page.authorName} <${page.authorEmail}>`})
        await this.git.push('origin', sanitizedBranchName)
      }
      await this.git.checkout(this.config.branch)
      await this.sync()
    } catch (error) {
      WIKI.logger.error('(STORAGE/GIT) Error updating page:', error)
      throw error
    }
  }

  async deleted(page) {

  }

  async assetUploaded (asset) {
    WIKI.logger.info(`(STORAGE/GIT) Committing new file ${asset.path}...`)
    const filePath = path.join(this.repoPath, asset.path)
    await fs.outputFile(filePath, asset.data, 'utf8')

    await this.git.add(`./${asset.path}`)
    await this.git.commit(`docs: upload ${asset.path}`, asset.path, {
      '--author': `"${asset.authorName} <${asset.authorEmail}>"`
    })
  }
  async assetDeleted (asset) {
    WIKI.logger.info(`(STORAGE/GIT) Committing removed file ${asset.path}...`)
    await this.git.rm(`./${asset.path}`)
    await this.git.commit(`docs: delete ${asset.path}`, asset.path, {
      '--author': `"${asset.authorName} <${asset.authorEmail}>"`
    })
  }
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
}

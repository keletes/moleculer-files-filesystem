"use strict";

const Promise	= require("bluebird");
const { MoleculerError, ServiceSchemaError } = require("moleculer").Errors;
const fs = Promise.promisifyAll(require("fs"));
const path = require("path");
const uuidv4 = require("uuid/v4");
const readdir = require("recursive-readdir");

class FSAdapter {

	constructor(uri, opts, dbName) {
	  this.uri = uri;
		this.opts = opts;
	}

	init(broker, service) {
		this.broker = broker;
		this.service = service;

		if (!this.uri) {
  		throw new ServiceSchemaError("Missing `uri` definition!");
    }

		if (!this.service.schema.collection) {
			/* istanbul ignore next */
			throw new ServiceSchemaError("Missing `collection` definition in schema of service!");
		}
		this.collection = this.service.schema.collection;
	}

	async connect() {
    await fs.lstatAsync(path.join(this.uri, this.service.schema.collection));
		return;
  }

	disconnect() {
		return Promise.resolve();
	}

  // Checks if the file to access is in the defined subdirectory; returns an error otherwise
  checkIsInDir(file) {
  	const relative = path.relative(path.join(this.uri, this.collection), path.join(this.uri, this.collection, file));
  	if (relative && !relative.startsWith('..')) return;
  	throw new MoleculerError('You are trying to access an unauthorized path.', 403, 'E_UNAUTHORIZED');
	}

	async find(filters) {
		const list = await readdir(path.join(this.uri, this.collection));
		return list.map((file) => {return path.relative(path.join(this.uri, this.collection), file)}).filter((file) => true);
	}

	findOne(query) {
		// To be implemented
		return;
	}

  findById(fd) {
    this.checkIsInDir(fd);
	  const stream = fs.createReadStream(path.join(this.uri, this.collection, fd));

	  return new Promise((resolve, reject) => {
  	  stream.on('open', () => resolve(stream));
  	  stream.on('error', (err) => resolve(null));
	  });

	}

	async count(filters = {}) {
		const list = await this.find(filters);
		return list.length;
	}

	save(entity, meta) {
  	const filename = meta.id || uuidv4();
  	this.checkIsInDir(filename);
  	return new Promise(async (resolve, reject) => {
    	try {
    	  await fs.accessAsync(path.dirname(path.join(this.uri, this.collection, filename)));
      } catch(e) {
        if (e.code == 'ENOENT') {
          try {
            await fs.mkdirAsync(path.dirname(path.join(this.uri, this.collection, filename)), {recursive: true});
          } catch(e) {
            if (e.code != 'EEXIST') throw e;
          }
        }
        else throw e;
      }

  		const s = fs.createWriteStream(path.join(this.uri, this.collection, filename));
      entity.pipe(s);
      s.on('finish', () => resolve(s));
    });
	}

	async updateById(entity, meta) {
		return await this.save(entity, meta);
	}

	removeMany(query) {
		// To Be Implemented.
	}

	async removeById(_id) {
  	this.checkIsInDir(_id);
  	try {
  		await fs.unlinkAsync((path.join(this.uri, this.collection, _id)));
  		return Promise.resolve({id: _id});
    } catch(e) {
      return null;
    }
	}

	clear() {
		return this.collection.deleteMany({}).then(res => res.deletedCount);
	}
}

module.exports = FSAdapter;
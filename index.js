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

	/**
	 * Find all entities by filters.
	 *
	 * Available filter props:
	 * 	- limit
	 *  - offset
	 *  - sort
	 *  - search
	 *  - searchFields
	 *  - query
	 *
	 * @param {Object} filters
	 * @returns {Promise<Array>}
	 *
	 * @memberof FSAdapter
	 */
	async find(filters) {
		const list = await readdir(path.join(this.uri, this.collection));
		return list.map((file) => {return path.relative(path.join(this.uri, this.collection), file)}).filter((file) => true);
	}

	/**
	 * Find an entity by query
	 *
	 * @param {Object} query
	 * @returns {Promise}
	 * @memberof FSAdapter
	 */
	findOne(query) {
		// To be implemented
		return;
	}

	/**
	 * Find an entities by ID.
	 *
	 * @param {String} _id
	 * @returns {Promise<Object>} Return with the found document.
	 *
	 * @memberof FSAdapter
	 */
  findById(fd) {
	  const stream = fs.createReadStream(path.join(this.uri, this.collection, fd));

	  return new Promise((resolve, reject) => {
  	  stream.on('open', () => resolve(stream));
  	  stream.on('error', (err) => resolve(null));
	  });

	}

	/**
	 * Get count of filtered entites.
	 *
	 * Available query props:
	 *  - search
	 *  - searchFields
	 *  - query
	 *
	 * @param {Object} [filters={}]
	 * @returns {Promise<Number>} Return with the count of documents.
	 *
	 * @memberof FSAdapter
	 */
	async count(filters = {}) {
		const list = await this.find(filters);
		return list.length;
	}

	/**
	 * Insert an entity.
	 *
	 * @param {Object} entity
	 * @returns {Promise<Object>} Return with the inserted document.
	 *
	 * @memberof FSAdapter
	 */
	save(entity, meta) {
  	return new Promise(async (resolve, reject) => {
    	const filename = meta.id || uuidv4();
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

	/**
	 * Update an entity by ID and `update`
	 *
	 * @param {String} _id - ObjectID as hexadecimal string.
	 * @param {Object} update
	 * @returns {Promise<Object>} Return with the updated document.
	 *
	 * @memberof FSAdapter
	 */
	async updateById(entity, meta) {
		return await this.save(entity, meta);
	}

	/**
	 * Remove entities which are matched by `query`
	 *
	 * @param {Object} query
	 * @returns {Promise<Number>} Return with the count of deleted documents.
	 *
	 * @memberof FSAdapter
	 */
	removeMany(query) {
		// To Be Implemented.
	}

	/**
	 * Remove an entity by ID
	 *
	 * @param {String} _id - ObjectID as hexadecimal string.
	 * @returns {Promise<Object>} Return with the removed document.
	 *
	 * @memberof FSAdapter
	 */
	async removeById(_id) {
  	try {
  		await fs.unlinkAsync((path.join(this.uri, this.collection, _id)));
  		return Promise.resolve({id: _id});
    } catch(e) {
      return null;
    }
	}

	/**
	 * Clear all entities from collection
	 *
	 * @returns {Promise}
	 *
	 * @memberof FSAdapter
	 */
	clear() {
		return this.collection.deleteMany({}).then(res => res.deletedCount);
	}
}

module.exports = FSAdapter;
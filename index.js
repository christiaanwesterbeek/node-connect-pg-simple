'use strict';

const url = require('url');
const util = require('util');
const oneDay = 86400;

const currentTimestamp = function () {
  return Math.ceil(Date.now() / 1000);
};

module.exports = function (session) {
  const Store = session.Store || session.session.Store;

  const PGStore = function (options) {
    options = options || {};
    Store.call(this, options);

    this.schemaName = options.schemaName || null;
    this.tableName = options.tableName || 'session';
    if (!options.pgPromise) {
      throw new Error('`pgPromise` is required for this fork');
    }
    this.columns = {
      sid: options.fields && options.fields.sid || 'sid',
      sess: options.fields && options.fields.sess || 'sess',
      expire: options.fields && options.fields.expire || 'expire',
    }

    this.ttl = options.ttl;

    if (options.pool !== undefined) {
      this.pool = options.pool;
      this.ownsPg = false;
    } else if (options.pgPromise !== undefined) {
      if (typeof options.pgPromise.query !== 'function') {
        throw new Error('`pgPromise` config must point to an existing and configured instance of pg-promise pointing at your database');
      }
      this.pgPromise = options.pgPromise;
      this.ownsPg = false;
    } else {
      const conString = options.conString || process.env.DATABASE_URL;
      let conObject = options.conObject;

      if (!conString && !conObject) {
        throw new Error('No database connecting details provided to connect-pg-simple');
      }

      if (!conObject) {
        const params = url.parse(conString);
        const auth = params.auth ? params.auth.split(':') : [];
        const port = params.port ? parseInt(params.port, 10) : undefined;
        const database = (params.pathname || '').split('/')[1];

        conObject = {
          user: auth[0],
          password: auth[1],
          host: params.hostname,
          port: port,
          database: database
        };
      }

      this.pool = new (require('pg')).Pool(conObject);
      this.ownsPg = true;
    }

    this.errorLog = options.errorLog || console.error.bind(console);

    if (options.pruneSessionInterval === false) {
      this.pruneSessionInterval = false;
    } else {
      this.pruneSessionInterval = (options.pruneSessionInterval || 60) * 1000;
      setImmediate(function () {
        this.pruneSessions();
      }.bind(this));
    }
  };

  /**
   * Inherit from `Store`.
   */

  util.inherits(PGStore, Store);

  /**
   * Closes the session store
   *
   * Currently only stops the automatic pruning, if any, from continuing
   *
   * @access public
   */

  PGStore.prototype.close = function () {
    this.closed = true;

    if (this.pruneTimer) {
      clearTimeout(this.pruneTimer);
      this.pruneTimer = undefined;
    }

    if (this.ownsPg) {
      this.pool.end();
    }
  };

  /**
   * Does garbage collection for expired session in the database
   *
   * @param {Function} [fn] - standard Node.js callback called on completion
   * @access public
   */

  PGStore.prototype.pruneSessions = function (fn) {
    this.query(
      'DELETE FROM ' + this.quotedTable() + ' WHERE $1~ < to_timestamp($2)', [
      this.columns.expire, currentTimestamp()
    ], function (err) {
      if (fn && typeof fn === 'function') {
        return fn(err);
      }

      if (err) {
        this.errorLog('Failed to prune sessions:', err.message);
      }

      if (this.pruneSessionInterval && !this.closed) {
        if (this.pruneTimer) {
          clearTimeout(this.pruneTimer);
        }
        this.pruneTimer = setTimeout(this.pruneSessions.bind(this, true), this.pruneSessionInterval);
        this.pruneTimer.unref();
      }
    }.bind(this));
  };

  /**
   * Get the quoted table.
   *
   * @return {String} the quoted schema + table for use in queries
   * @access private
   */

  PGStore.prototype.quotedTable = function () {
    let result = '"' + this.tableName + '"';

    if (this.schemaName) {
      result = '"' + this.schemaName + '".' + result;
    }

    return result;
  };

  /**
   * Figure out when a session should expire
   *
   * @param {Number} [maxAge] - the maximum age of the session cookie
   * @return {Number} the unix timestamp, in seconds
   * @access private
   */

  PGStore.prototype.getExpireTime = function (maxAge) {
    let ttl = this.ttl;

    ttl = ttl || (typeof maxAge === 'number' ? maxAge / 1000 : oneDay);
    ttl = Math.ceil(ttl + currentTimestamp());

    return ttl;
  };

  /**
   * Query the database.
   *
   * @param {String} query - the database query to perform
   * @param {(Array|Function)} [params] - the parameters of the query or the callback function
   * @param {Function} [fn] - standard Node.js callback returning the resulting rows
   * @access private
   */

  PGStore.prototype.query = function (query, params, fn) {
    if (!fn && typeof params === 'function') {
      fn = params;
      params = [];
    }

    if (this.pgPromise) {
      this.pgPromise.query(query, params || [])
        .then(function (res) { fn && fn(null, res && res[0] ? res[0] : false); })
        .catch(function (err) { fn && fn(err, false); });
    } else {
      this.pool.query(query, params || [], function (err, res) {
        if (fn) { fn(err, res && res.rows[0] ? res.rows[0] : false); }
      });
    }
  };

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid – the session id
   * @param {Function} fn – a standard Node.js callback returning the parsed session object
   * @access public
   */

  PGStore.prototype.get = function (sid, fn) {
    this.query('SELECT $5~ FROM ' + this.quotedTable() + ' WHERE $1~ = $2 AND $3~ >= to_timestamp($4)', [
      this.columns.sid, sid, this.columns.expire, currentTimestamp(), this.columns.sess
    ], function (err, data) {
      if (err) { return fn(err); }
      if (!data) { return fn(); }
      try {
        const sessValue = data[this.columns.sess];
        return fn(null, (typeof sessValue === 'string') ? JSON.parse(sessValue) : sessValue);
      } catch (e) {
        return this.destroy(sid, fn);
      }
    }.bind(this));
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid – the session id
   * @param {Object} sess – the session object to store
   * @param {Function} fn – a standard Node.js callback returning the parsed session object
   * @access public
   */

  PGStore.prototype.set = function (sid, sess, fn) {
    const self = this;
    const expireTime = this.getExpireTime(sess.cookie.maxAge);
    const query = 'INSERT INTO ' + self.quotedTable() + ' ($1~, $3~, $5~) SELECT $2, to_timestamp($4), $6 ON CONFLICT ($5~) DO UPDATE SET $1~=$2, $3~=to_timestamp($4) RETURNING $5~';
    this.query(
      query, [
      this.columns.sess, sess, this.columns.expire, expireTime, this.columns.sid, sid
    ], function (err, data) {
      if (fn) { fn.apply(this, err); }
    });
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid – the session id
   * @access public
   */

  PGStore.prototype.destroy = function (sid, fn) {
    this.query(
      'DELETE FROM ' + this.quotedTable() + ' WHERE $1~ = $2', [
      this.columns.sid, sid
    ], function (err) {
      if (fn) { fn(err); }
    });
  };

  /**
   * Touch the given session object associated with the given session ID.
   *
   * @param {String} sid – the session id
   * @param {Object} sess – the session object to store
   * @param {Function} fn – a standard Node.js callback returning the parsed session object
   * @access public
   */

  PGStore.prototype.touch = function (sid, sess, fn) {
    const expireTime = this.getExpireTime(sess.cookie.maxAge);

    this.query(
      'UPDATE ' + this.quotedTable() + ' SET $1~ = to_timestamp($2) WHERE $3~ = $4 RETURNING $3~', [
        this.columns.expire, expireTime, this.columns.sid, sid],
      function (err) { fn(err); }
    );
  };

  return PGStore;
};

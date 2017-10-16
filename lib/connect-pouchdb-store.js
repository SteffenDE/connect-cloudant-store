/**
 * Connect - PouchDB Store
 * Copyright(c) 2017 Steffen Deusch
 *
 * MIT Licensed
 *
 * This is a fork from connect-cloudant-store, see:
 * https://github.com/adriantanasa/connect-cloudant-store
 */

var util = require("util");
var _ = require("lodash");
var PouchDB = require("pouchdb");
var noop = function() {};

function withCallback(promise, cb) {
  promise
    .then(res => cb(null, res))
    .catch(cb)
  return promise
}


/**
 * Return the `PouchDBStore` extending `express`'s session Store.
 *
 * @param {object} express session
 * @return {Function}
 * @api public
 */
module.exports = function(session) {
  var self;

  /**
   * One day in seconds.
   */
  var oneDay = 86400;

  var getTTL = function(store, sess) {
    var maxAge = sess.cookie && sess.cookie.maxAge ? sess.cookie.maxAge : null;
    return store.ttl || (typeof maxAge === "number" ? Math.floor(maxAge / 1000) : oneDay);
  };

  var sessionToDb = function(sid, sess, ttl) {
    var dbData = _.assign({}, JSON.parse(JSON.stringify(sess)),
      {_id: sid, session_ttl: ttl, session_modified: Date.now()});
    return dbData;
  };

  /**
   * Express's session Store.
   */
  var Store = session.Store;

  /**
   * Initialize PouchDBStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */
  function PouchDBStore(options) {
    if (!(this instanceof PouchDBStore)) {
      throw new TypeError("Cannot call PouchDBStore constructor as a function");
    }
    self = this;
    options = options || {};
    Store.call(this, options);

    this.pouchOptions = options.pouchOptions || "sessions";
    this.prefix = options.prefix || "sess:";
    this.disableTTLRefresh = options.disableTTLRefresh || false;
    this.dbViewName = options.dbViewName || "express_expired_sessions";
    this.dbDesignName = options.dbDesignName || "expired_sessions";
    this.dbRemoveExpMax = options.dbRemoveExpMax || 100;
    if (options.database && options.database instanceof PouchDB) {
      this.db = options.database;
    }
    else {
      this.db = new PouchDB(this.pouchOptions);
    }
    this.checkClientConnection();
  }

  /**
   * Inherit from `Store`.
   */
  util.inherits(PouchDBStore, Store);

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {sessCallback} fn
   * @api public
   */
  PouchDBStore.prototype.get = function(sid, fn) {
    console.log("GET \"%s\"", sid);
    fn = fn || noop;

    withCallback(new Promise((resolve, reject) => {
      self.db.get(self.prefix + sid).then(function(data) {
        if (data.session_modified + data.session_ttl * 1000 < Date.now()) {
          console.log("GET \"%s\" expired session", sid);
          self.destroy(sid, fn);
        }
        else {
          console.log("GET \"%s\" found rev \"%s\"", sid, data._rev);
          resolve(data);
        }
      }).catch(function(err) {
        if (err.status === 404) {
          console.log("GET - SESSION NOT FOUND \"%s\"", sid);
          resolve(null);
          return;
        }
        reject(err);
      });
    }), fn);
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {sessCallback} fn
   * @api public
   */
  PouchDBStore.prototype.set = function(sid, sess, fn) {
    console.log("SET")
    fn = fn || noop;

    withCallback(new Promise((resolve, reject) => {
      // read current _rev
      self.db.get(self.prefix + sid).then(doc => {
        var newDoc = Object.assign(doc, sessionToDb(self.prefix + sid, sess, getTTL(self, sess)));
        console.log("SET session \"%s\" rev \"%s\"", sid, sess._rev);
        self.db.put(newDoc).then(function() {
          resolve();
        }).catch(function(err) {
          console.log("SET session error \"%s\" rev \"%s\" err \"%s\"", sid, sess._rev,
            JSON.stringify(err));
          self.emit("error", err);
          reject(err);
        });
      }).catch(err => {
        if (err.status === 404) {
          console.log("SET session \"%s\" rev \"%s\"", sid, sess._rev);
          self.db.put(sessionToDb(self.prefix + sid, sess, getTTL(self, sess))).then(function() {
            resolve();
          }).catch(function(err) {
            console.log("SET session error \"%s\" rev \"%s\" err \"%s\"", sid, sess._rev,
              JSON.stringify(err));
            self.emit("error", err);
            reject(err);
          });
        }
      });
    }), fn);
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @param {sessCallback} fn
   * @api public
   */
  PouchDBStore.prototype.destroy = function(sid, fn) {
    console.log("DESTROY session \"%s\"", sid);
    fn = fn || noop;

    withCallback(new Promise((resolve, reject) => {
      // get _rev needed for delete
      // TODO check why db.head is not working
      self.db.get(self.prefix + sid).then(function(data) {
        // cleanup expired sessions
        self.db.remove(self.prefix + sid, data._rev).then(res => {
          resolve();
        }).catch(function(err) {
          console.log("DESTROY - DB error \"%s\" rev \"%s\" err \"%s\"", sid, data._rev,
            JSON.stringify(err));
          self.emit("error", err);
          reject(err);
        });
      }).catch(function(err) {
        console.log("DESTROY - DB GET failure \"%s\" err \"%s\"", sid, JSON.stringify(err));
        self.emit("error", err);
        reject(err);
      });
    }), fn);
  };

  /**
   * Refresh the time-to-live for the session with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {sessCallback} fn
   * @api public
   */
  PouchDBStore.prototype.touch = function(sid, sess, fn) {
    fn = fn || noop;

    withCallback(new Promise((resolve, reject) => {
      if (self.disableTTLRefresh) {
        resolve();
        return;
      }
      console.log("TOUCH session \"%s\" rev \"%s\"", sid, sess._rev);

      self.db.get(self.prefix + sid).then(function(data) {
        // update TTL
        sess._rev = data._rev;
        self.db.put(sessionToDb(self.prefix + sid, data, getTTL(self, sess))).then(function() {
          resolve();
        })
        .catch(function(err) {
          console.log("TOUCH session error \"%s\" rev \"%s\" err \"%s\"", sid, sess._rev,
            JSON.stringify(err));
          self.emit("error", err);
          reject(err);
        });
      }).catch(function(err) {
        console.log("TOUCH - error on returning the session \"%s\" rev \"%s\" err \"%s\"",
          sid, sess._rev, JSON.stringify(err));
        self.emit("error", err);
        reject(err);
      });
    }), fn);
  };

  PouchDBStore.prototype.checkClientConnection = function() {
    return self.db.info().then(function() {
      self.emit("connect");
    }).catch(function(err) {
      self.emit("disconnect");
      console.log("DATABASE does not exists %s", JSON.stringify(err));
    });
  };

  PouchDBStore.prototype.cleanupExpired = function() {
    return new Promise(function(resolve, reject) {
      self.initView().then(function() {
        self.db.view(self.dbDesignName, self.dbViewName, {limit: self.dbRemoveExpMax}).then(function(body) {
          // bulk delete
          if (body.total_rows > 0) {
            var delRows = [];
            body.rows.forEach(function(row) {
              delRows.push(
                {
                  _id: row.key,
                  _rev: row.value,
                  _deleted: true
                }
              );
            });

            console.log("cleanupExpired - BULK delete %s", JSON.stringify(body.rows));
            self.db.bulk({docs: delRows}, null).then(function() {
              resolve();
            }).catch(function(err) {
              reject(err);
            });
          }
          else {
            console.log("cleanupExpired - Nothing to delete");
            resolve();
          }
        }).catch(function(err) {
          console.log("cleanupExpired - ERROR reading expired sessions", JSON.stringify(err));
          reject(err);
        });
      }).catch(function(err) {
        console.log("cleanupExpired - Failed to load/create views", JSON.stringify(err));
        reject(err);
      });
    });
  };

  PouchDBStore.prototype.initView = function() {
    return new Promise(function(resolve, reject) {
      self.db.view(self.dbDesignName, self.dbViewName, {limit: 0}).then(function() {
        resolve();
      }).catch(function(err) {
        if (err.status === 404) {
          // try to create DB
          console.log("View for expired session doesn't exists - Try to create");
          var designDoc = {"views": {}};
          designDoc["views"][self.dbViewName] = {
            "map": function(doc) {
              if (doc.session_ttl && doc.session_modified &&
              Date.now() > (doc.session_ttl + doc.session_modified)) {
              // eslint-disable-next-line
              emit(doc._id, doc._rev);
              }
            }
          };

          self.db.insert(designDoc, "_design/" + self.dbDesignName).then(function() {
            resolve();
          }).catch(function(err) {
            reject(err);
          });
        }
        else {
          reject(err);
        }
      });
    });
  };

  return PouchDBStore;
};

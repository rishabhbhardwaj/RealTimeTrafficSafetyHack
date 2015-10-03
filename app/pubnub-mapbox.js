var eon = eon || {};
eon.subsub = eon.subsub || subsub;
eon.m = {
  create: function (options) {

    if(typeof(PUBNUB) == "undefined" && console) {
      return console.error("PubNub not found. See http://www.pubnub.com/docs/javascript/javascript-sdk.html#_where_do_i_get_the_code");
    }

    if(typeof(options.mb_token) == "undefined" && console) {
      return console.error("Please supply a Mapbox Token: https://www.mapbox.com/help/create-api-access-token/");
    }

    if(typeof(options.mb_id) == "undefined" && console) {
      return console.error("Please supply a Mapbox Map ID: https://www.mapbox.com/help/define-map-id/");
    }

    if(typeof(L) == "undefined" && console) {
      return console.error("You need to include the Mapbox Javascript library.");
    }

    var self = this;

    L.mapbox.accessToken = options.mb_token;

    var geo = {
      bearing : function (lat1,lng1,lat2,lng2) {
        var dLon = this._toRad(lng2-lng1);
        var y = Math.sin(dLon) * Math.cos(this._toRad(lat2));
        var x = Math.cos(this._toRad(lat1))*Math.sin(this._toRad(lat2)) - Math.sin(this._toRad(lat1))*Math.cos(this._toRad(lat2))*Math.cos(dLon);
        var brng = this._toDeg(Math.atan2(y, x));
        return ((brng + 360) % 360);
      },
      _toRad : function(deg) {
         return deg * Math.PI / 180;
      },
      _toDeg : function(rad) {
        return rad * 180 / Math.PI;
      }
    };

    self.pubnub = options.pubnub || PUBNUB || false;

    if(!self.pubnub) {
      error = "PubNub not found. See http://www.pubnub.com/docs/javascript/javascript-sdk.html#_where_do_i_get_the_code";
    }

    options.id = options.id || false;
    options.channel = options.channel || false;
    options.transform = options.transform || function(m){return m};
    options.history = options.history || false;
    options.message = options.message || function(){};
    options.connect = options.connect || function(){};
    options.rotate = options.rotate || false;
    options.marker = options.marker || L.marker;
    options.options = options.options || {};

    self.markers = {};

    if(!options.id) {
      return console.error('You need to set an ID for your Mapbox element.');
    }

    self.map = L.mapbox.map(options.id, options.mb_id, options.options);

    self.refreshRate = options.refreshRate || 10;

    self.lastUpdate = new Date().getTime();

    self.update = function (seed, animate) {

      for(var key in seed) {

        if(!self.markers.hasOwnProperty(key)) {

          var data = seed[key].data || {};

          self.markers[key]= options.marker(seed[key].latlng, seed[key].data);
          self.markers[key].addTo(self.map);

        } else {

          if(animate) {
            self.animate(key, seed[key].latlng);
          } else {
            self.updateMarker(key, seed[key].latlng);
          }

        }

      }

      self.lastUpdate = new Date().getTime();

    };

    var isNumber = function(n) {
      return !isNaN(parseFloat(n)) && isFinite(n);
    };

    self.updateMarker = function (index, point) {

      if(point && point.length > 1) {

        if(isNumber(point[0]) && isNumber(point[1])) {
          self.markers[index].setLatLng(point);
        }

      }

    };

    self.animations = {};

    self.animate = function (index, destination) {

      var startlatlng = self.markers[index].getLatLng();

      self.animations[index] = {
        start: startlatlng,
        dest: destination,
        time: new Date().getTime(),
        length: new Date().getTime() - self.lastUpdate
      };

    };

    self.refresh = function() {

      for(var index in self.markers) {

        if(typeof self.animations[index] !== 'undefined') {

          // number of steps in this animations
          var maxSteps = Math.round(self.animations[index].length / self.refreshRate)

          // time that has passed since that message
          var timeSince = new Date().getTime() - self.animations[index].time;
          var numSteps = Math.round(timeSince / self.refreshRate)

          var position = self.animations[index].start;

          var lat = position.lat + ((self.animations[index].dest[0] - position.lat) / maxSteps) * numSteps;
          var lng = position.lng + ((self.animations[index].dest[1] - position.lng) / maxSteps) * numSteps;

          var nextStep = [lat, lng];

          self.updateMarker(index, nextStep);

          if(options.rotate) {
            self.markers[index].options.angle = geo.bearing(position.lat, position.lng, lat, lng);
          }

        }

        index++;

      }

    };

    subsub.subscribe(self.pubnub, options.channel, false, function(message, env, channel) {

      message = options.transform(message);

      options.message(message, env, channel);
      self.update(message, true);

    });

    if(options.history) {

      self.pubnub.history({
        channel: options.channel,
        count: 1,
        callback: function(m) {

          if(m[0].length) {
            self.update(m[0][0], true);
          }

          options.connect();

        }
     });

    } else {
      options.connect();
    }

    self.refresh();
    setInterval(self.refresh, self.refreshRate);

    return self.map;

  }
};
eon.map = function(o) {
  return new eon.m.create(o);
};

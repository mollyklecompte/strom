/*
Factory function for Strom Javascript client.
This methods allows more flexibility for users to inherit/extend methods w/o typical JS hiccups.
Uses only native Javascript.

author: Adrian Agnic <adrian@tura.io>
*/
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ping_r = new XMLHttpRequest();

const StromClient = ({
    url='http://127.0.0.1:5000',
    tokens={}
  } = {}) => ({
  url,
  tokens,

  _setUrl(url) {
    this.url = url;
  },
  _setToken(name, token) {
    this.tokens[name] = token;
  },
  _ping() {
    // Sends GET request to server index route. Response should be 'STROM-API is UP'.
    // const ping_r = new XMLHttpRequest();
    ping_r.onreadystatechange = () => {
      if (ping_r.readyState === XMLHttpRequest.DONE) {
        if (ping_r.status === 200) {
          console.log(ping_r.responseText);
        }
      }
    };
    ping_r.open('GET', this.url);
    ping_r.send();
  },

  register_device(name, template) {
    // POSTS template to server and returns stream_token for this device.
    thus = this;
    const ping_r = new XMLHttpRequest();
    ping_r.onreadystatechange = function () {
      if (ping_r.readyState === XMLHttpRequest.DONE) {
        if (ping_r.status === 200) {
          let result = ping_r.responseText;
          console.log(result);
          while (thus.tokens[name] == undefined) {
            thus._setToken(name, result);
          };
          return true;
        }
      }
    };
    ping_r.open('POST', this.url + '/api/define');
    ping_r.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    ping_r.send('template=' + encodeURIComponent(template));
  },

  register_event(token, callback) {
    // Sends token and callback function to server
    //TODO
  },

  send(token, data) {
    const send_r = new XMLHttpRequest();
    send_r.open('POST', this.url + '/api/kafka/load', true);
    send_r.onreadystatechange = () => {
      if (send_r.readyState === XMLHttpRequest.DONE) {
        if (send_r.status === 202) {
          return send_r.responseText;
        }
      }
    };
    send_r.send();
  }


});
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

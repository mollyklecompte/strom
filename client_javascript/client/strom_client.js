/*
Factory function for Strom Javascript client.
This methods allows more flexibility for users to inherit/extend methods w/o typical JS hiccups.
Uses only native Javascript + socket.io.

author: Adrian Agnic <adrian@tura.io>
*/
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

const StromClient = ({url='http://127.0.0.1:5000', /*socket=io(url),*/ tokens={}} = {}) => ({
  url,
  //socket,
  tokens,

  _setToken(name, token) {
    this.tokens[name] = token;
  },
  _ping() {
    let ping_r = new XMLHttpRequest();
    ping_r.open('GET', this.url, true);
    ping_r.onreadystatechange = function() {
      if (ping_r.readyState === 4) {
        if (ping_r.status === 200) {
          console.log(ping_r.responseText);
        }
      }
    };
    ping_r.send();
  },
  tokenizeData(name, data) {
    let token = this.tokens[name];
    str_data = JSON.stringify(data);
    json_data = JSON.parse(str_data);
    for (let i = 0;i <= json_data.length;i++) {
      json_data['stream_token'] = token;
    }
    return JSON.stringify(json_data);
  },
  registerDevice(name, template) {
    thus = this;
    let regDev_r = new XMLHttpRequest();
    regDev_r.open('POST', this.url + '/api/define', false);
    regDev_r.onreadystatechange = function() {
      if (regDev_r.readyState === 4) {
        if (regDev_r.status === 200) {
          console.log('Registration Success.');
          thus._setToken(name, regDev_r.responseText);
        }
      }
    };
    regDev_r.setRequestHeader('Content-type', 'application/x-www-form-urlencoded');
    regDev_r.send('template=' + encodeURIComponent(template));
  },
  registerEvent(name, callback) {
    // TODO
  },
  send(name, data) {
    let token_data = this.tokenizeData(name, data);
    send_r = new XMLHttpRequest();
    send_r.open('POST', this.url + '/api/kafka/load', true);
    send_r.onreadystatechange = function() {
      if (send_r.readyState === 4) {
        if (send_r.status === 202) {
          console.log('Data sent.');
        }
      }
    };
    send_r.setRequestHeader('Content-type', 'application/x-www-form-urlencoded');
    send_r.send('stream_data=' + encodeURIComponent(token_data));
  }
});
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

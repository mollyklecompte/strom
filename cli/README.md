### Strom CLI

###### Author/Developer:
*Adrian Agnic <[adrian@tura.io](http://tura.io)>*

##### Description:
Command-line interface tool for use with Strom-API.

#### Pre-Requisites:
*   Python3
*   [Click](http://click.pocoo.org/)
    *   *CLI building tool*
*   [Requests](http://docs.python-requests.org/en/master/)
    *   *HTTP protocol library for Python*

#### Download:
*   Clone Repository
```commandline
git clone git@github.com:tura-io/strom.git
```

#### Install:
* ``` pip install strom-cli ```
*   Navigate to project directory
```commandline
    cd strom/cli/
```
*   Install using Pip
```commandline
    pip install .
```
*   (Optional) Enable auto-complete in Bash
    ```commandline
    eval "$(_DSTREAM_COMPLETE=source dstream)"
    ```

#### Run:
```commandline
dstream --help
```

##### Example Usage:
```commandline
dstream define -template demo_data/demo_template
```
```commandline
dstream load -filepath demo_data/demo_data_log -token tokenized_template
```
```commandline
dstream events --all -token tokenized_template
```

##### *INCOMING*:
  *   'GET' methods for data
    *   ``` dstream raw ```
    *   ``` dstream filtered ```
    *   ``` dstream derived_params ```
  * ``` dstream add_source ```

#### Help:
* ``` --help ``` can be given to each command for more information

# Cool Package
## Package Overview

### Description
*Description of the module’s purpose, content, and functionality.*

### Use in Project
*Describe how, specifically, the contents of this module are used in the larger project. If it contains a class, where is that class instantiated? If it contains helper functions, where are those functions called?*

### Module Contents
```class AThing```
<br>*brief description of class*

###### Attributes
```name```
string, *the name of the AThing instance* (passed in init)

```dated_created```
int, *timestamp when AThing instance created* (defined at init)

```dated_modified```
int, *timestamp when AThing instance last modified* (init as None)

```data_out```
list, *a list of processed datas for output* (init as empty list)

###### Methods
```_get_callback(self, process_key, callback_dict)```<br>
```process_key```: string, *lookup key for callback function*
```callback_dict```: dict, *rules and callbacks for data processing*

*Value of `process_key` looks ups up to key in `callback_dict`with value of a callback function. Looks up & returns callback function in `callback_dict` corresponding to value of `process_key`.*
<br>Returns: *callback function* (function)

```process_data(data)```<Br>
```data```: dict, input data

*Calls `self._get_callback` to get proper function to apply to data. Calls this function, passing `**data` in as kwargs, returning the processed data & appending it to `self.data_out`.*


Returns: *Processed data* (dict)


##Package Dev History
*A place to casually track of the historical development of the package, following the examples below*

**Date**<br>
Dev name<br>
Branch changes were made on<br>
*Brief, high level description of changes to modules in package (include names, not details about changes in functionality)*

**2/1/17**<br>
Molly<br>
center-core<br>
*Removed `useless` attribute from class AThing*
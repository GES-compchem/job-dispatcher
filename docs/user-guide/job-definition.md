# Job definition
Jobs can be defined by using the Job class.

% for styling tables see this link https://stackoverflow.com/questions/36121672/set-table-column-width-via-markdown/57420043#57420043?newreg=995a7ad145334227b4c84bbcba4b652f

````{admonition} Function signature
:class: note
`jobdispatcher.Job(name="", function=<no default function>, arguments=[], keyword_arguments={}, cores=1)`
````
```{admonition} Parameters
:class: note
|<div style="width:150px">Parameter</div> | <div style="width:350px">Description</div>   |
|---|---|
| <div style="width:150px">**name** (str)</div>  | <div style="width:450px">Name of the job. Must be unique as it allow to retrieve the job result.</div>|
| **function** (Callable) | The function to be executed by the job |
```


````{admonition} Job Class
:class: dropdown
```{eval-rst}
.. autoclass:: jobdispatcher.processor.Job
```
````


```python
a = 5
print(a)

b = "blablabla"

def function(bao):
    print(bao)

```



# Job depende
ncies
Not implemented yet.
# A Galaxy of Learn Data - Blast off with BbData & Python!
## Bb DevCon 20 

### Setup
First we need to install the snowflake-sqlalchemy module (along with some other modules). Use the following command in your terminal of choice:

```
python3 -m pip install --upgrade --user snowflake-sqlalchemy panda matplotlib numpy docopt pymongo
```

Or you can just do the following to allow the requirements.txt do the work for you:

```
python3 -m pip install --user -r requirements.txt
```


* Note: You may have to adjust the command above to fit your environment.
* Tip: https://docs.snowflake.com/en/user-guide/sqlalchemy.html


If all goes well, you should be able to follow along and run the provided scripts or start hacking away at your own queries!


## Some tips on writing queries for this project:
* First, create folder in the queries folder
    - Dashes (or underscores) instead of spaces
* Second, create a query.sql file
    - You can use {} opened and closed curly brackets to inject variables
* Third, create a variables.json files with the following syntax:
```
{
    "variables": {
            "your key": "string" || int || [list] || obj{}
    }
}    
```
    - The key will be replace the {key}:{value} that was placed in the query.sql

## Cached data after initial run or Data file does not exist
When you run your query it checks for a previously saved file called data.csv. If this file does not exist, then one is created. If the file does exist, then that file is return instead.

## Saving Output
You can save your plot data by adding plt.savefig('./exports/NameOfOutput.svg'). Of course, you can change this to whever you wish to save your plots.
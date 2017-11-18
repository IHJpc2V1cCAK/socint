# Twitter

Twitter specific scripts & utilities.

This is a mess, it's a hackjob for now.

```
# Create & edit the config file. For now there's no database involved, so db
#   settings aren't needed. The twitter consumer and access values are needed.
#   You'll need a twitter account and to create an app to get these API keys.
~/socint/twitter/collect$ cp ./config.conf.sample ./config.conf
~/socint/twitter/collect$ gedit ./config.conf

# Create a virtual environment and install requirements
~/socint/twitter/collect$ virtualenv --python=/usr/bin/python3 ./env
~/socint/twitter/collect$ . ./env/bin/activate
(env)~/socint/twitter/collect$ pip install -r ./requirements.txt
    ...
    ...
```

## user_timeline.py

Collects a twitter user's time line and outputs to CSV file.

```
# Use the script
(env)~/socint/twitter/collect$ ./user_timeline.py --user jack --output ./jack.csv
    ...
    ...
```


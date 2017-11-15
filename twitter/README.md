# Twitter

Twitter specific scripts & utilities.

This is a mess, it's a hackjob for now.

## user_timeline.py

Collects a twitter user's time line.

```
~/socint/twitter/collect$ virtualenv --python=/usr/bin/python3 ./env
~/socint/twitter/collect$ . ./env/bin/activate
(env)~/socint/twitter/collect$ pip install -r ./requirements.txt
    ...
    ...
(env)~/socint/twitter/collect$ ./user_timeline.py -u jack -o ./jack.csv
    ...
    ...
```


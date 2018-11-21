# TwitPeep

Generate various reports using Twitter Streaming API.

* Note:- Works with python 2 only for now.

### Prerequisites

You need to have Python on your system.

If you are going to use virtualenv to create standalone environment, make sure you have it set up
```
pip install virtualenv
```


## How to Run

1. Open cmd and make a directory where you'd be cloning the repo.
```
  mkdir twit_dir
```
* if you are not going to use virtualenv, you can directly clone the repo without having to make a directory.

2. Next, go to the directory you just created and set up a virtual environemnt. If you are not using virtualenv, you can skip to step 4.
```
cd twit_dir
virtualenv twit_env
```
* This will create another directory where all the dependencies will be set.

3. Next, activate the virtual environment.
```
source twit_env/bin/activate
```

4. Now, clone the repo by running
```
git clone https://github.com/Zlash65/TwitPeep.git
```

5. Next step is to set up all the requirements to run the project
```
cd TwitPeep
pip install -r requirements.txt
```

6. Once all the requirements are set up, you need to start the program by
```
python twitter_streamer.py
```

* Note:- Before running it, you need to have all the credentials set up. In the `twitter_credentials.py` file you can see the empty variables that needs to be set. Make a developer account on twitter and fill the tokens from the data there. Also, the program uses mongodb to store its data while its streaming twitter data. You can set up your account on https://mlab.com and create a database for free. Make a database user after creating the database. You'll be given all the necessary details then that you simply need to put in its appropriate holder in the credentials file.

* To deactivate the virtualenv, simply run `deactivate` in terminal

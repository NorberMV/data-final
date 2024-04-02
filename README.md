# Entregable-4

This creates a lightweight and functional script that can be used on any operating system by any user. 
It runs a Directed Acyclic Graph (DAG) that retrieves Bitcoin data from the CoinGecko API, processes 
the data into a pandas DataFrame, and then inserts it into a Redshift database.
It also dockerizes a script to make it functional on any operating system.

- [How can I install `entregable-4`?](#how-can-i-install-entregable-4)
- [How can I run this ?](#how-can-i-run-this?)

# How can I install `entregable-4`?
Just make sure you have the following installed and set up on your machine:

- You need to have Docker Desktop and Docker Compose installed.
- Create a ./logs folder at the root of the repository.
- This assumes you have an `.env` file at the root of the repository containing the following Redshift DB data:
```
# Redshift connection config
DB_NAME=
HOST=
PORT=
USERNAME=
PASSW=

# email alert config
EMAIL_TO=
```
- You need to configure the following [smtp] settings on the airflow.cfg file with your smtp creds.
```  
[smtp]
# If you want airflow to send emails on retries, failure, and you want to use
# the airflow.utils.email.send_email_smtp function, you have to configure an
# smtp server here

smtp_host = smtp.gmail.com

smtp_starttls = True

smtp_ssl = False

smtp_user = norbermv.dev@gmail.com

smtp_password = dwfhhzeboowptxql

smtp_port = 25

smtp_mail_from = norbermv.dev@gmail.com

smtp_timeout = 30

smtp_retry_limit = 5

```


# How can I run this?
- Launch Docker desktop to have the docker daemon running.
- Now you can run the command below to run the build.
```
docker compose up --build
```
- Once the build is done, please go to `localhost:8080` in whatever browser you want.
- Input `airflow` as the user and `airflow`for the password . 
- Once inside, before activate the DAG, please add a new Redshift connection as follows: 
  - to do so go to Admin/Connections on the Airflow ui.
  - Fill the connection fields provided in the `.env` shared file, please use `"redshift_coder"` as the connection Id. 
- Finally activate the DAG, wait for it to turn dark green so that means the pipeline has run.
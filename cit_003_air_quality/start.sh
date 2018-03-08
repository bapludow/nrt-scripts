#Change the NAME variable with the name of your script
<<<<<<< HEAD:cit_003_air_quality/start.sh
NAME=air-quality
=======
NAME=conflict_protest
LOG=${LOG:-udp://localhost}
>>>>>>> c9958496d854c9a96a0ba517f21c8b55ff9ca872:soc_016_conflict_protest_events/start.sh

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME -v $(pwd)/data:/opt/$NAME/data --env-file .env --rm $NAME python main.py

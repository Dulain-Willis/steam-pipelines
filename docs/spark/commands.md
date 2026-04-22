# Spark Commands

## Jobs

List Spark job files deployed to a container (useful for verifying a rebuilt image contains the expected job files):

    docker exec spark-master ls /opt/spark/jobs/replication/

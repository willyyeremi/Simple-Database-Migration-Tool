mkdir ".\database\data\postgresql"
mkdir ".\database\data\mysql"
mkdir ".\database\data\oracledb"
@REM docker pull container-registry.oracle.com/database/express:latest
@REM docker tag container-registry.oracle.com/database/express:latest oracledb:latest
@REM docker rmi container-registry.oracle.com/database/express:latest
docker-compose -p mockup_database up -d
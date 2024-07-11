# Install vfproxy from JAR
# NOTE: make sure to copy/link JAR in this directory
JAR_PATH=dependencies/vfproxy/vfproxy.jar

mvn install:install-file \
   -Dfile=${JAR_PATH} \
   -DgroupId=vfproxy \
   -DartifactId=vfproxy \
   -Dversion=1.0 \
   -Dpackaging=jar

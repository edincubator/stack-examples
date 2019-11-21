### read-hdfs example

Put your keytab file on the workdir, build the image and push it:

```
    docker build --build-arg USER_KEYTAB=<username>.service.keytab -t registry.edincubator.eu/<username>/read-hdfs:v1 .
    docker push registry.edincubator.eu/<username>/read-hdfs:v1
```

The Keytab file should have a+r access. To launch the example:

```
    yarn jar /opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.1.1.jar -jar /opt/hadoop/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-3.1.1.jar -shell_env YARN_CONTAINER_RUNTIME_TYPE=docker -shell_env YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=registry.edincubator.eu/<username>/read-hdfs:v1 -shell_env YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro,/etc/krb5.conf:/etc/krb5.conf:ro -shell_command /source/launch.sh -shell_args <username>.service.keytab -shell_args <username>@EDINCUBATOR.EU -shell_args http://edincubator-m-3-20191031113524.c.edi-call2.internal:50070 -shell_args /user/<username>/<path-to-file>
```
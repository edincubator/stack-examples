import sys
from hdfs.ext.kerberos import KerberosClient

client = KerberosClient(sys.argv[1])

with client.read(sys.argv[2]) as reader:
    test = reader.read()

print(test)
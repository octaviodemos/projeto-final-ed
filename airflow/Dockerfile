FROM astrocrpublic.azurecr.io/runtime:3.2-5

USER root
RUN python <<'PY'
import os
import urllib.request
import pyspark

jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
os.makedirs(jars_dir, exist_ok=True)

jars = {
    "hadoop-aws-3.3.4.jar": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
}

for name, url in jars.items():
    path = os.path.join(jars_dir, name)
    if not os.path.exists(path):
        urllib.request.urlretrieve(url, path)
PY
USER astro

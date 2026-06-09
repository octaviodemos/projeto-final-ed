import os
import urllib.request
import pyspark

def main():
    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    os.makedirs(jars_dir, exist_ok=True)
    
    jars_to_download = {
        "hadoop-aws-3.3.4.jar": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
        "aws-java-sdk-bundle-1.12.262.jar": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    }

    for jar_name, url in jars_to_download.items():
        jar_path = os.path.join(jars_dir, jar_name)
        if not os.path.exists(jar_path):
            print(f"Baixando {jar_name}...")
            urllib.request.urlretrieve(url, jar_path)
            print(f"{jar_name} baixado com sucesso.")
        else:
            print(f"{jar_name} já existe.")

if __name__ == "__main__":
    main()

---
__Resumo do Projeto__

Projeto desenvolvido para o challenge-data-engineering MaisTodos

---
### Arquitetura
![Arquiteture](arquiteture.jpg)


#### How to Execute the project

Imagem Airflow retirada do https://github.com/puckel/docker-airflow

Com o DockerHub intalado execute:

    docker-compose -f docker-compose-LocalExecutor.yml up -d


entre no terminal:

    docker exec -u root -t -i containerID /bin/bash

execute:

    pip install --upgrade pip
    pip install boto3

Apos criar o connector com a aws passando seu usuário e criar os buckets no s3 execute a dag.

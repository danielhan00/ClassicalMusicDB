pip install --target ./package spotipy pymysql tidalapi retry
cd package && zip -r ../my-deployment-package.zip ./*
cd .. && cp get_popularity_today.py lambda_function.py
zip -g my-deployment-package.zip lambda_function.py
aws lambda update-function-code --function-name clasically-get-popularity --zip-file fileb://my-deployment-package.zip
rm lambda_function.py
rm my-deployment-package.zip

mkdir package
pip install --target package pymysql
cd package
zip -r ../lambda_function.zip .
cd ..
zip lambda_function.zip lambda_function.py
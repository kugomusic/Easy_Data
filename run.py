#!flask/bin/python
from app import app
app.run(debug=True, host='10.108.211.130', port=8993)
# app.run(debug=True, host='0.0.0.0', port=8993)
# app.run(debug=True, host='127.0.0.1', port=8993)
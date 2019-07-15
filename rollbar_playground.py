import os

# !pip install -U python-dotenv
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

#! pip install rollbar
import rollbar

ROLLBAR = os.getenv('ROLLBAR')
rollbar.init(ROLLBAR)
rollbar.report_message('Rollbar is configured correctly')

dir(rollbar)

try:
    b = a + 1
except:
    rollbar.report_exc_info()

class ModelIsGarbage(Exception):
   pass

score = 0.05
threshold = 0.10

if score >= threshold:
    print('Yay!')
else:
    raise ModelIsGarbage(f'score ({score}) is below acceptable threshold ({threshold})')

from datetime import datetime
from random import choice
product=['HRCL','IPSH','CRCL']
print(str(datetime.now().year)[2:]+choice(product))
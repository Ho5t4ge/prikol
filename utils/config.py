from datetime import datetime
import warnings as warnings

field_ids = {'NE': 86, 'MI': 78}
search_date = datetime.strptime('2024-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f')
warnings.simplefilter('ignore')

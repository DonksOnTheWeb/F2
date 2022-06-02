import pandas as pd
from prophet import Prophet


def forecast(params):
    df = pd.io.json.json_normalize(params.get('history'))
    m = Prophet(uncertainty_samples=0)
    #if ('holiday_locale' in params):
    #    m.add_country_holidays(country_name=params.get('holiday_locale'))

    m.fit(df)

    future = m.make_future_dataframe(periods=params.get('periods'))
    return m.predict(future)

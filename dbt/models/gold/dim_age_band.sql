select column1::varchar as age_band_key, column2::number as min_age, column3::number as max_age
from values
    ('18-29', 18, 29),
    ('30-39', 30, 39),
    ('40-49', 40, 49),
    ('50+', 50, 100)

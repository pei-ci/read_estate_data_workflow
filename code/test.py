import real_estate_data_workflow
import pandas as pd
import numpy as np

data = real_estate_data_workflow.Data('../real_estate_data/')
data.process_data()
data_df_all = data.get_df_all()
data_auto = real_estate_data_workflow.Data('../real_estate_data_test/')
data_auto.process_data()
data_auto_df_all = data_auto.get_df_all()
unique_content = data_df_all["land sector position building sector house number plate"].unique(
)

unique_content_auto = data_auto_df_all["land sector position building sector house number plate"].unique(
)

result = np.setdiff1d(unique_content, unique_content_auto)
print(result)

result = np.setdiff1d(unique_content_auto, unique_content)
print(result)

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the CSV file into a Pandas DataFrame
healthcare_data_pd = pd.read_csv("/Users/nidhimishra/Downloads/healthcare_dataset.csv")

# Printing column names to verify
print(healthcare_data_pd.columns)
sns.histplot(healthcare_data_pd['Age'], bins=30, kde=True)
plt.xlabel('Age')
plt.ylabel('Frequency')
plt.title('Age Distribution')
plt.show()
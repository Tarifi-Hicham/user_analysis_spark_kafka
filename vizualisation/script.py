from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
from pymongo import MongoClient


# MongoDB connection details
mongo_uri = "mongodb://localhost:27017"
mongo_db = "users_db"
mongo_collection = "users_collection"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]

# Retrieve data from MongoDB
data_from_mongo = list(collection.find())

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data_from_mongo)


# Create a Dash application
app = Dash(__name__)

# Define the layout of the dashboard
app.layout = html.Div([
    html.H1(children='nat per gender', style={'textAlign':'center'}),
    dcc.Graph(id='graph-userscount'),
    dcc.Graph(id='graph-avgage'),
    dcc.Graph(id='graph-domain'),
])


@callback(
    Output('graph-userscount', 'figure'),
    Input('graph-userscount', 'figure'),
)
def update_graph(value):
    # Get users count by gender and nationality
    df_nat = df.groupby(['nationality','gender'])['nationality'].count().reset_index(name='count')

    return px.bar(df_nat, x='nationality', y='count', color='gender', \
                  barmode='group', color_discrete_map = {'male': 'blue', 'female': 'pink'}, \
                  title="Users count by nationality and gender")


@callback(
    Output('graph-avgage', 'figure'),
    Input('graph-avgage', 'figure'),
)
def update_graph(value):
    # Calculate the average age for all genders
    average_age_all = df['age'].mean().astype(int)

    # Calculate the average age for males
    average_age_male = df[df['gender'] == 'male']['age'].mean().astype(int)

    # Calculate the average age for females
    average_age_female = df[df['gender'] == 'female']['age'].mean().astype(int)

    # Create a new DataFrame
    df_avgage = pd.DataFrame({
        'Gender': ['All', 'Male', 'Female'],
        'Average Age': [average_age_all, average_age_male, average_age_female]
    })

    return px.bar(df_avgage, x= 'Gender', y= 'Average Age')


@callback(
    Output('graph-domain', 'figure'),
    Input('graph-domain', 'figure'),
)
def update_graph(value):
    # Count the occurrences of each domain name
    domain_counts = df['email_domaine'].value_counts()

    return px.pie(domain_counts, names=domain_counts.index, values=domain_counts.values)

if __name__ == '__main__':
    app.run(debug=True)

import dash
import pandas as pd
import sqlite3
from dash import dcc, html
from dash.dependencies import Input, Output
from dash import dash_table

# Create a Dash application
app = dash.Dash(__name__)

# Connect to the SQLite database
conn = sqlite3.connect('./data/olist.sqlite')
# Retrieve all table names
res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_names = [name[0] for name in res.fetchall()]
conn.close()

# App layout
app.layout = html.Div([
    dcc.Dropdown(
        id='table-dropdown',
        options=[{'label': name, 'value': name} for name in table_names],
        value=table_names[0]  # Default value
    ),
    html.Div(id='table-container')
])

# Callback to update the data table based on selected table name
@app.callback(
    Output('table-container', 'children'),
    Input('table-dropdown', 'value')
)
def update_table(selected_table):
    conn = sqlite3.connect('your_database_file.sqlite')
    df = pd.read_sql_query(f"SELECT * FROM {selected_table}", conn)
    conn.close()
    return dash_table.DataTable(
        data=df.to_dict('records'),
        columns=[{'name': col, 'id': col} for col in df.columns]
    )

# Run the app
if __name__ == '__main__':
    app.run(debug=True)

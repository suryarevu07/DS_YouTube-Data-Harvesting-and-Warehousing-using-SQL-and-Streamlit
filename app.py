import dash #The framework for the web app
from dash import dcc, html, dash_table #Components for dropdowns
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc # for stylish
import pandas as pd
import requests #To call the backend API
import plotly.express as px

#Dash app-Bootstrap
app = dash.Dash(__name__, external_stylesheets=[
    'https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css',
    'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap'
], suppress_callback_exceptions=True)


API_BASE_URL = "http://localhost:5000" #(running on port 5000)

#memorycache
cache = {} #Avoids redundant API calls, improving performance

#dropdown
DATA_TYPES = [
    {'label': 'Channel Details', 'value': 'channel'},
    {'label': 'Playlist Details', 'value': 'playlist'},
    {'label': 'Video Details', 'value': 'video'},
    {'label': 'Comment Details', 'value': 'comment'}
]

#styles
CUSTOM_CSS = {
    'background': 'linear-gradient(135deg, #1a1a1a 0%, #2c3e50 100%)',
    'textColor': '#ffffff',
    'accentColor': '#00f7ff',
    'secondaryColor': '#ff6f61',
    'cardBg': 'rgba(255, 255, 255, 0.05)',
    'fontFamily': 'Inter, sans-serif'
}

#Layout
app.layout = html.Div(
    style={
        'background': CUSTOM_CSS['background'],
        'minHeight': '100vh',
        'color': CUSTOM_CSS['textColor'],
        'fontFamily': CUSTOM_CSS['fontFamily'],
        'padding': '20px'
    },
    children=[
        html.H1('YouTube Data Explorer', className='display-4 fw-bold text-center mb-4', style={'color': CUSTOM_CSS['accentColor']}),
        html.P(
            'Use the tabs below to select and fetch specific data, fetch all data, or analyze stored data.',
            className='text-center mb-4',
            style={'color': CUSTOM_CSS['textColor'], 'opacity': '0.8'}
        ),
        dbc.Tabs([  #Dash Bootstrap Components to create 3 tabs
            dbc.Tab(
                label='Data Selection',
                tab_id='selection', #Default active tab is "Data Selection"
                tab_style={'color': CUSTOM_CSS['textColor'], 'fontWeight': '500'},
                active_tab_style={'backgroundColor': CUSTOM_CSS['accentColor'], 'color': '#000', 'fontWeight': '600'}
            ),
            dbc.Tab(
                label='Fetch All Data',
                tab_id='all',
                tab_style={'color': CUSTOM_CSS['textColor'], 'fontWeight': '500'},
                active_tab_style={'backgroundColor': CUSTOM_CSS['accentColor'], 'color': '#000', 'fontWeight': '600'}
            ),
            dbc.Tab(
                label='Analysis',
                tab_id='analysis',
                tab_style={'color': CUSTOM_CSS['textColor'], 'fontWeight': '500'},
                active_tab_style={'backgroundColor': CUSTOM_CSS['accentColor'], 'color': '#000', 'fontWeight': '600'}
            )
        ], id='tabs', active_tab='selection', className='mb-4'),
        html.Div(id='tabs-content')
        #placeholder container. When a tab is clicked, its corresponding content will be rendered here dynamically using a callback like
        
    ]
)

# Callback-rendering
#a callback is a function that automatically gets called
# when the user interacts with the app
@app.callback( #This callback dynamically updates
    Output('tabs-content', 'children'),
    Input('tabs', 'active_tab')
)
def render_tab_content(tab):
    if tab == 'selection': #user select a specific data type to fetch (channel, video, comment, etc.).
        return dbc.Card(
            dbc.CardBody([
                html.H2('Data Selection', className='h2 mb-4'),
                html.Label('Select Data Type:', className='form-label mb-2'),
                dcc.Dropdown(
                    id='data-type-dropdown',
                    options=DATA_TYPES,
                    value='channel',
                    className='form-select mb-3',
                    style={'backgroundColor': 'rgba(255, 255, 255, 0.1)', 'color': CUSTOM_CSS['textColor']}
                ),
                html.Label('Enter YouTube Channel ID:', className='form-label mb-2'),
                dbc.Input(
                    id='channel-id',
                    type='text',
                    placeholder='e.g., UCpOWnfbrnVjhdZ0OVgvTFpg',
                    className='form-control mb-3',
                    style={
                        'backgroundColor': 'rgba(255, 255, 255, 0.1)',
                        'color': CUSTOM_CSS['textColor'],
                        'borderColor': CUSTOM_CSS['accentColor']
                    }
                ),
                dbc.Button(
                    'Fetch Data',
                    id='fetch-button',
                    n_clicks=0,
                    className='btn mb-3',
                    style={'backgroundColor': CUSTOM_CSS['secondaryColor'], 'border': 'none'}
                ),
                dcc.Loading(
                    id='fetch-loading',
                    type='circle',
                    children=html.Div(id='fetch-status', className='mt-3')
                ),
                html.Div(id='data-table', className='mt-3'),
                html.Div(className='d-flex gap-3 mt-3', children=[
                    dbc.Button(
                        'Store in MongoDB',
                        id='mongo-button',
                        n_clicks=0,
                        className='btn',
                        style={'backgroundColor': CUSTOM_CSS['accentColor'], 'border': 'none'}
                    ),
                    dbc.Button(
                        'Store in MySQL',
                        id='mysql-button',
                        n_clicks=0,
                        className='btn',
                        style={'backgroundColor': '#6c5ce7', 'border': 'none'}
                    ),
                ]),
                dcc.Loading(
                    id='store-loading',
                    type='circle',
                    children=html.Div(id='store-status', className='mt-3')
                ),
            ]),
            style={'backgroundColor': CUSTOM_CSS['cardBg'], 'border': 'none', 'color': CUSTOM_CSS['textColor']},
            className='shadow-lg p-4'
        )
    elif tab == 'all':#Fetch channel + playlist + video + comment data all together.
        return dbc.Card(
            dbc.CardBody([
                html.H2('Fetch All Data', className='h2 mb-4'),
                html.Label('Enter YouTube Channel ID:', className='form-label mb-2'),
                dbc.Input(
                    id='all-channel-id',
                    type='text',
                    placeholder='e.g., UCpOWnfbrnVjhdZ0OVgvTFpg',
                    className='form-control mb-3',
                    style={
                        'backgroundColor': 'rgba(255, 255, 255, 0.1)',
                        'color': CUSTOM_CSS['textColor'],
                        'borderColor': CUSTOM_CSS['accentColor']
                    }
                ),
                dbc.Button(
                    'Fetch All Data',
                    id='fetch-all-button',
                    n_clicks=0,
                    className='btn mb-3',
                    style={'backgroundColor': CUSTOM_CSS['secondaryColor'], 'border': 'none'}
                ),
                dcc.Loading(
                    id='fetch-all-loading',
                    type='circle',
                    children=html.Div(id='fetch-all-status', className='mt-3')
                ),
                html.Div(id='all-data-table', className='mt-3'), #Div: Display results
                html.Div(className='d-flex gap-3 mt-3', children=[
                    dbc.Button(
                        'Store in MongoDB',
                        id='all-mongo-button',
                        n_clicks=0,
                        className='btn',
                        style={'backgroundColor': CUSTOM_CSS['accentColor'], 'border': 'none'}
                    ),
                    dbc.Button(
                        'Store in MySQL',
                        id='all-mysql-button',
                        n_clicks=0,
                        className='btn',
                        style={'backgroundColor': '#6c5ce7', 'border': 'none'}
                    ),
                ]),
                dcc.Loading(
                    id='store-all-loading',
                    type='circle',
                    children=html.Div(id='store-all-status', className='mt-3')
                ),
            ]),
            style={'backgroundColor': CUSTOM_CSS['cardBg'], 'border': 'none', 'color': CUSTOM_CSS['textColor']},
            className='shadow-lg p-4'
        )
    elif tab == 'analysis':#Allow users to run SQL-like queries against stored data.
        questions = [
            "Names of all videos and their corresponding channels",
            "Channels with the most number of videos and their counts",
            "Top videos and their channels",
            "Number of comments for each video and their video names",
            "Videos with the highest number of likes and their channels",
            "Total number of likes for each video and their video names",
            "Total number of views for each channel",
            "Channels that published videos in 2022",
            "Average duration of videos per channel",
            "Videos with the highest number of comments and their channels"
        ]
        return dbc.Card(
            dbc.CardBody([
                html.H2('Data Analysis', className='h2 mb-4'),
                html.Label('Select a Query:', className='form-label mb-2'),
                dcc.Dropdown(
                    id='question-dropdown',
                    options=[{'label': q, 'value': q} for q in questions],
                    value=questions[0],
                    className='form-select mb-3',
                    style={'backgroundColor': 'rgba(255, 255, 255, 0.1)', 'color': CUSTOM_CSS['textColor']}
                ),
                dbc.Button(
                    'Run Query',
                    id='query-button',
                    n_clicks=0,
                    className='btn mb-3',
                    style={'backgroundColor': CUSTOM_CSS['secondaryColor'], 'border': 'none'}
                ),
                dcc.Loading(
                    id='query-loading',
                    type='circle',
                    children=[ #Loading: Display query result
                        html.Div(id='query-results', className='mt-3'),
                        dcc.Graph(id='query-graph', className='mt-3')
                    ]
                ),
            ]),
            style={'backgroundColor': CUSTOM_CSS['cardBg'], 'border': 'none', 'color': CUSTOM_CSS['textColor']},
            className='shadow-lg p-4'
        )

#Callback for-dropdown-fetch
# Callback Declaration
@app.callback(
    [Output('fetch-status', 'children'), Output('data-table', 'children')],
    Input('fetch-button', 'n_clicks'),
    [State('data-type-dropdown', 'value'), State('channel-id', 'value')]
)
def fetch_dropdown_data(n_clicks, data_type, channel_id):
    if not n_clicks or not channel_id or not data_type:
        return '', '' # do nothing. when no click 
    
    status = dbc.Alert('Fetching data...', color='warning', dismissable=False)
    try:
        #Sends a GET request to the backend Flask/Quart API.
        response = requests.get(f"{API_BASE_URL}/fetch/{channel_id}?type={data_type}")
        response.raise_for_status()
        data = response.json()
        if 'error' in data:
            return dbc.Alert(data['error'], color='danger', dismissable=True), ''
        
        cache[f'{data_type}_data'] = data
        df = pd.DataFrame(list(data.values())[0])
        table = dash_table.DataTable(
            id='data-table',
            data=df.to_dict('records'),
            columns=[{'name': col, 'id': col} for col in df.columns],
            style_table={'overflowX': 'auto'},
            page_size=5,
            style_cell={
                'textAlign': 'left',
                'backgroundColor': 'transparent',
                'color': CUSTOM_CSS['textColor'],
                'padding': '10px'
            },
            style_header={
                'backgroundColor': CUSTOM_CSS['accentColor'],
                'fontWeight': 'bold',
                'color': '#000'
            }
        )
        return dbc.Alert(f'{data_type.title()} data fetched successfully!', color='success', dismissable=True), table
    except requests.RequestException as e:
        return dbc.Alert(f"Error fetching data: {str(e)}", color='danger', dismissable=True), ''

# Callback-dropdown data storage
#this is single store mongo or sql 
@app.callback(
    Output('store-status', 'children'),
    [Input('mongo-button', 'n_clicks'), Input('mysql-button', 'n_clicks')],
    [State('data-type-dropdown', 'value'), State('channel-id', 'value')]
)
def store_dropdown_data(mongo_clicks, mysql_clicks, data_type, channel_id):
    ctx = dash.callback_context
    if not ctx.triggered or not channel_id or not data_type:
        return ''
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    status = dbc.Alert('Storing data...', color='warning', dismissable=False)
#Stores all fetched data in MongoDB or MySQL, with a status update
    try:
        data = cache.get(f'{data_type}_data', {})
        if not data:
            response = requests.get(f"{API_BASE_URL}/fetch/{channel_id}?type={data_type}")
            response.raise_for_status()
            data = response.json()
            cache[f'{data_type}_data'] = data
        
        if button_id == 'mongo-button' and mongo_clicks > 0:
            response = requests.post(f"{API_BASE_URL}/store/mongodb", json=data)
            response.raise_for_status()
            result = response.json()
            return dbc.Alert(result['message'], color='success' if result['status'] == 'success' else 'danger', dismissable=True)
        elif button_id == 'mysql-button' and mysql_clicks > 0:
            response = requests.post(f"{API_BASE_URL}/store/mysql", json=data)
            response.raise_for_status()
            result = response.json()
            return dbc.Alert(result['message'], color='success' if result['status'] == 'success' else 'danger', dismissable=True)
    except requests.RequestException as e:
        return dbc.Alert(f"Error storing data: {str(e)}", color='danger', dismissable=True)
    
    return ''

# Callback for fetch all data
@app.callback(
    [Output('fetch-all-status', 'children'), Output('all-data-table', 'children')],
    Input('fetch-all-button', 'n_clicks'),
    State('all-channel-id', 'value')
)
def fetch_all_data(n_clicks, channel_id):
    if not n_clicks or not channel_id:
        return '', ''
    
    status = dbc.Alert('Fetching all data...', color='warning', dismissable=False)
    try:
        response = requests.get(f"{API_BASE_URL}/fetch/{channel_id}?type=all")
        response.raise_for_status()
        data = response.json()
        if 'error' in data:
            return dbc.Alert(data['error'], color='danger', dismissable=True), ''
        
        cache['all_data'] = data
        tables = []
        for key in ['channel_details', 'playlist_details', 'video_details', 'comment_details']:
            if key in data and data[key]:
                df = pd.DataFrame(data[key])
                tables.append(
                    html.Div([
                        html.H4(key.replace('_details', '').title(), className='mt-4'),
                        dash_table.DataTable(
                            id=f'{key}-table',
                            data=df.to_dict('records'),
                            columns=[{'name': col, 'id': col} for col in df.columns],
                            style_table={'overflowX': 'auto'},
                            page_size=5,
                            style_cell={
                                'textAlign': 'left',
                                'backgroundColor': 'transparent',
                                'color': CUSTOM_CSS['textColor'],
                                'padding': '10px'
                            },
                            style_header={
                                'backgroundColor': CUSTOM_CSS['accentColor'],
                                'fontWeight': 'bold',
                                'color': '#000'
                            }
                        )
                    ])
                )
        return dbc.Alert('All data fetched successfully!', color='success', dismissable=True), tables
    except requests.RequestException as e:
        return dbc.Alert(f"Error fetching data: {str(e)}", color='danger', dismissable=True), ''

# Callback for all data storage
@app.callback(
    Output('store-all-status', 'children'),
    [Input('all-mongo-button', 'n_clicks'), Input('all-mysql-button', 'n_clicks')],
    State('all-channel-id', 'value')
)
def store_all_data(mongo_clicks, mysql_clicks, channel_id):
    ctx = dash.callback_context
    if not ctx.triggered or not channel_id:
        return ''
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    status = dbc.Alert('Storing all data...', color='warning', dismissable=False)
#Stores all fetched data in MongoDB or MySQL, with a status update
    try:
        data = cache.get('all_data', {})
        if not data:
            response = requests.get(f"{API_BASE_URL}/fetch/{channel_id}?type=all")
            response.raise_for_status()
            data = response.json()
            cache['all_data'] = data
        
        if button_id == 'all-mongo-button' and mongo_clicks > 0:
            response = requests.post(f"{API_BASE_URL}/store/mongodb", json=data)
            response.raise_for_status()
            result = response.json()
            return dbc.Alert(result['message'], color='success' if result['status'] == 'success' else 'danger', dismissable=True)
        elif button_id == 'all-mysql-button' and mysql_clicks > 0:
            response = requests.post(f"{API_BASE_URL}/store/mysql", json=data)
            response.raise_for_status()
            result = response.json()
            return dbc.Alert(result['message'], color='success' if result['status'] == 'success' else 'danger', dismissable=True)
    except requests.RequestException as e:
        return dbc.Alert(f"Error storing data: {str(e)}", color='danger', dismissable=True)
    
    return ''

# Callback for analysis
@app.callback(
    [Output('query-results', 'children'), Output('query-graph', 'figure')],
    Input('query-button', 'n_clicks'),
    State('question-dropdown', 'value')
)
def run_analysis(n_clicks, selected_question):
    if not n_clicks or not selected_question:
        return None, px.bar() # nothing 
    
    queries = {
        "Names of all videos and their corresponding channels": "SELECT video_name, channel_name FROM video_details",
        "Channels with the most number of videos and their counts": "SELECT channel_name, COUNT(*) as video_count FROM video_details GROUP BY channel_name ORDER BY video_count DESC",
        "Top videos and their channels": "SELECT video_name, channel_name, view_count FROM video_details ORDER BY view_count DESC LIMIT 5",
        "Number of comments for each video and their video names": "SELECT video_name, comment_count FROM video_details WHERE comment_count > 0",
        "Videos with the highest number of likes and their channels": "SELECT video_name, channel_name, like_count FROM video_details ORDER BY like_count DESC LIMIT 5",
        "Total number of likes for each video and their video names": "SELECT video_name, like_count FROM video_details WHERE like_count > 0",
        "Total number of views for each channel": "SELECT channel_name, SUM(view_count) as total_views FROM video_details GROUP BY channel_name",
        "Channels that published videos in 2022": "SELECT DISTINCT channel_name FROM video_details WHERE YEAR(published_at) = 2022",
        "Average duration of videos per channel": """
            SELECT channel_name, AVG(TIME_TO_SEC(STR_TO_DATE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(duration, 'PT', ''),
                    '([HMS])', ':$1'
                ), '%H:%i:%s'
            ))) as avg_duration_seconds FROM video_details GROUP BY channel_name
        """,
        "Videos with the highest number of comments and their channels": "SELECT video_name, channel_name, comment_count FROM video_details ORDER BY comment_count DESC LIMIT 5"
    }
    
    try:
        response = requests.post(f"{API_BASE_URL}/query", json={'query': queries[selected_question]})
        response.raise_for_status()
        result = response.json()
        if not result:
            return dbc.Alert("No results found for the query", color='warning', dismissable=True), px.bar()
        
        df = pd.DataFrame(result)
        table = dash_table.DataTable(
            id='query-table',
            data=df.to_dict('records'),
            columns=[{'name': col, 'id': col} for col in df.columns],
            style_table={'overflowX': 'auto'},
            page_size=5,
            style_cell={
                'textAlign': 'left',
                'backgroundColor': 'transparent',
                'color': CUSTOM_CSS['textColor'],
                'padding': '10px'
            },
            style_header={
                'backgroundColor': CUSTOM_CSS['accentColor'],
                'fontWeight': 'bold',
                'color': '#000'
            }
        )
        
        fig = px.bar()
        if selected_question in [
            "Channels with the most number of videos and their counts",
            "Top videos and their channels",
            "Videos with the highest number of likes and their channels",
            "Total number of views for each channel",
            "Videos with the highest number of comments and their channels"
        ]:
            y_col = {
                'Channels with the most number of videos and their counts': 'video_count',
                'Top videos and their channels': 'view_count',
                'Videos with the highest number of likes and their channels': 'like_count',
                'Total number of views for each channel': 'total_views',
                'Videos with the highest number of comments and their channels': 'comment_count'
            }[selected_question]
            fig = px.bar(df, x='channel_name', y=y_col, text=y_col, title=selected_question,
                         color='channel_name', height=500, template='plotly_dark')
        elif selected_question == "Average duration of videos per channel":
            df['avg_duration'] = pd.to_timedelta(df['avg_duration_seconds'], unit='s')
            df['avg_duration'] = df['avg_duration'].apply(lambda x: f"{x.components.hours:02d}:{x.components.minutes:02d}:{x.components.seconds:02d}")
            table = dash_table.DataTable(
                id='query-table',
                data=df[['channel_name', 'avg_duration']].to_dict('records'),
                columns=[{'name': col, 'id': col} for col in ['channel_name', 'avg_duration']],
                style_table={'overflowX': 'auto'},
                page_size=5,
                style_cell={
                    'textAlign': 'left',
                    'backgroundColor': 'transparent',
                    'color': CUSTOM_CSS['textColor'],
                    'padding': '10px'
                },
                style_header={
                    'backgroundColor': CUSTOM_CSS['accentColor'],
                    'fontWeight': 'bold',
                    'color': '#000'
                }
            )
            fig = px.bar(df, x='channel_name', y='avg_duration_seconds', text='avg_duration',
                         title=selected_question, color='channel_name', height=500, template='plotly_dark')
        
        return table, fig
    except requests.RequestException as e:
        return dbc.Alert(f"Error running query: {str(e)}", color='danger', dismissable=True), px.bar()
    except Exception as e:
        return dbc.Alert(f"Error in analysis: {str(e)}", color='danger', dismissable=True), px.bar()

if __name__ == '__main__':
    app.run(debug=True, port=8050) # allowing you to access the app at 8050

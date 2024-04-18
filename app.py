import calculate

import dash
import dash_daq as daq
import dash_core_components as dcc
import dash_html_components as html

import plotly.express as px


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server


available_strategies = ['Antonacci', 'TSMOM']
available_price_data = ['ETF', 'Futures']


# Application layout
app.layout = html.Div([
    html.Div([
        html.Div([
            dcc.Dropdown(
                id='strategy',
                options=[{'label': i, 'value': i} for i in available_strategies],
                value=available_strategies[0]
            ),

            dcc.Dropdown(
                id='price-data',
                options=[{'label': i, 'value': i} for i in available_price_data],
                value=available_price_data[0]
            ),            
            html.Div(id='fee-rate-bps-display'),
            dcc.Slider(
                id='fee-rate-bps',
                className='slid',
                min=0,
                max=100,
                step=1,
                value=10,
                marks={}
            ),
            daq.BooleanSwitch(
                id='use-vol-weight',
                label="Weight by Inverse Volatility (Antonacci)",
                labelPosition="top",
                on=False
            )
        ], style={'width': '49%', 'display': 'inline-block'}),

        html.Div([
            html.Div(id='top-n-display'),
            dcc.Slider(
                id='top-n',
                className='slid',
                min=1,
                max=30,
                step=1,
                value=2,
                marks={}
            ),
            
            html.Div(id='target-vol-display'),
            dcc.Slider(
                id='target-vol',
                className='slid',
                marks={},
                min=1,
                max=100,
                step=1,
                value=40
            ),
            
            html.Div(id='lookback-period-display'),
            dcc.Slider(
                id='lookback-period',
                className='slid',
                min=1,
                max=24,
                step=1,
                value=6,
                marks={}
            )
        ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'})
    ], style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '10px 5px'
    }),
        
    html.Div([
        dcc.Graph(id='nav-plot'),
        dcc.Graph(id='weights-plot')
        
    ], style={'display': 'inline-block', 'width': '26%'}),
    
    html.Div([
        dcc.Graph(id='leverage-plot'),
        dcc.Graph(id='drawdown-plot')
        
    ], style={'display': 'inline-block', 'width': '26%'}),
    
    html.Div([
        dcc.Graph(id='volatility-plot'),
        dcc.Graph(id='turnover-plot')
    ], style={'display': 'inline-block', 'width': '26%'}),
    
    html.Div([
        html.Code(id='summary', style={'white-space': 'pre-wrap'}),
    ], style={'display': 'inline-block', 'width': '22%', 'vertical-align': 'top', 'margin-top': '30px'}),
])


@app.callback([
    dash.dependencies.Output('fee-rate-bps-display', 'children'),
    dash.dependencies.Output('top-n-display', 'children'),
    dash.dependencies.Output('target-vol-display', 'children'),
    dash.dependencies.Output('lookback-period-display', 'children')
], [
    dash.dependencies.Input('fee-rate-bps', 'value'),
    dash.dependencies.Input('top-n', 'value'),
    dash.dependencies.Input('target-vol', 'value'),
    dash.dependencies.Input('lookback-period', 'value')
])
def update_sliders(fee_rate_bps, get_top, target_vol, periods):
    """Return updated slider information based on input."""
    return [
        f'Fee rate basis points: {fee_rate_bps}',
        f'Cross Sectional Asset Count (Antonacci): {get_top}',
        f'Target asset volatility (TSMOM): {target_vol}',
        f'Number of lookback months: {periods}'
    ]


@app.callback([
    dash.dependencies.Output('nav-plot', 'figure'),
    dash.dependencies.Output('weights-plot', 'figure'),
    dash.dependencies.Output('leverage-plot', 'figure'),
    dash.dependencies.Output('drawdown-plot', 'figure'),
    dash.dependencies.Output('volatility-plot', 'figure'),
    dash.dependencies.Output('turnover-plot', 'figure'),
    dash.dependencies.Output('summary', 'children')
], [
    dash.dependencies.Input('strategy', 'value'),
    dash.dependencies.Input('price-data', 'value'),
    dash.dependencies.Input('fee-rate-bps', 'value'),
    dash.dependencies.Input('top-n', 'value'),
    dash.dependencies.Input('target-vol', 'value'),
    dash.dependencies.Input('lookback-period', 'value'),
    dash.dependencies.Input('use-vol-weight', 'on')
])
def update_plots(name, price_set, fee_rate_bps, get_top, target_vol, periods, vol_weight):
    """Return updated plots based on user input."""
    stats = calculate.stats_from_parameters(name, price_set, fee_rate_bps, get_top, target_vol, periods, vol_weight)
    
    # Price plots
    nav_plot = px.line(x=stats['nav'].index, y=stats['nav'].values, 
                       labels={'x': 'date', 'y': 'return'}, height=400)
    
    # Weights of individual assets
    stats['asset_class_weights'].index = stats['asset_class_weights'].index.set_names(['date'])
    weights = stats['asset_class_weights'].reset_index().melt(id_vars="date", var_name="class", value_name='weight')
    
    if any(weights.weight < 0):
        weights_plot = px.line(weights, x='date', y='weight', color='class', height=400)
    else:
        weights_plot = px.area(weights, x='date', y='weight', color='class', height=400)

        
    # Leverage of the portfolio
    leverage_plot = px.line(x=stats['leverage'].index, y=stats['leverage'].values, 
                            labels={'x': 'date', 'y': 'leverage'}, height=400)
    
    # Drawdown of the portfolio
    drawdown_plot = px.line(x=stats['drawdown'].index, y=stats['drawdown'].values, 
                            labels={'x': 'date', 'y': 'drawdown'}, height=400)
    
    # Rolling volatility of portfolio
    volatility_plot = px.line(x=stats['ewm_volatility'].index, y=stats['ewm_volatility'].values, 
                              labels={'x': 'date', 'y': 'volatility'}, height=400)

    # Rolling mean of portfolio turnover
    turnover_plot = px.line(x=stats['turnover_annual'].index, y=stats['turnover_annual'].values, 
                            labels={'x': 'date', 'y': 'turnover'}, height=400)
    
    summary = stats['summary'].__str__()
    
    return nav_plot, weights_plot, leverage_plot, drawdown_plot, volatility_plot, turnover_plot, summary


if __name__ == '__main__':
    app.run_server(debug=True)

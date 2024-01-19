import os
import base64
import plotly.graph_objects as go
import streamlit as st
import urllib.parse
import connection

# Show a streamlit image without using st.image (since it is disabled)
# Uses plotly and writes a png to the background.
def image_png(file):
    fully_qualified = os.path.join(os.path.dirname(os.path.abspath(__file__)), file)
    with open(fully_qualified, "rb") as f:
        png_data = f.read()

    png = "data:image/png;base64," + base64.b64encode(png_data).decode("utf-8")

    fig = go.Figure()

    layout = go.Layout(
        xaxis=dict(
            showgrid=False, zeroline=False, showticklabels=False, fixedrange=True
        ),
        showlegend=False,
        yaxis=dict(
            showgrid=False, zeroline=False, showticklabels=False, fixedrange=True
        ),
        images=[
            dict(
                source=png,
                xref="x",
                yref="y",
                x=0,
                y=7,
                sizex=7,
                sizey=7,
                sizing="fill",
                opacity=1,
                layer="above",
            )
        ],
    )
    fig.update_layout(layout)
    fig.update_layout(xaxis_range=[0, 7], yaxis_range=[0, 7])
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def image_svg(file):
    fully_qualified = os.path.join(os.path.dirname(os.path.abspath(__file__)), file)
    with open(fully_qualified, "r") as f:
        svg_image = urllib.parse.quote(f.read())

    fig = go.Figure()

    go.Layout(
        xaxis=dict(
            showgrid=False, zeroline=False, showticklabels=False, fixedrange=True
        ),
        showlegend=False,
        width=452,
        height=267,
        margin=dict(l=20, r=20, t=20, b=100),
        yaxis=dict(
            showgrid=False, zeroline=False, showticklabels=False, fixedrange=True
        ),
        images=[
            go.layout.Image(
                source="data:image/svg+xml;charset=utf-8," + svg_image,
                xref="paper",
                yref="paper",
                # xanchor='left',
                # yanchor='top',
                sizex=1,
                sizey=1,
                sizing="stretch",
                opacity=1,
                layer="above",
            )
        ],
    )

    fig.update_layout(
        xaxis=dict(
            showgrid=False, zeroline=False, showticklabels=False, fixedrange=True
        ),
        yaxis=dict(
            showgrid=False, zeroline=False, showticklabels=False, fixedrange=True
        ),
        width=300,  # Width of the chart in pixels
        height=200,  # Height of the chart in pixels
        margin=dict(t=0, b=0, l=0, r=0),  # Set all margins to 0
        images=[
            go.layout.Image(
                source="data:image/svg+xml;charset=utf-8," + svg_image,
                xref="paper",
                yref="paper",
                x=0,
                y=1,
                sizex=1,
                sizey=1,
                opacity=1,
                layer="above",
            )
        ],
    )
    # fig.update_layout(layout)
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None)
    # fig.update_layout(xaxis_range=[0, 7], yaxis_range=[0, 4])
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def chrome(name: str = None):
    if name is None:
        name = "Sundeck OpsCenter"
    else:
        name = f"Sundeck OpsCenter - {name}"
    region = connection.execute_select("select current_region()").values[0][0]

    st.set_page_config(layout="wide", page_title=name, page_icon=":pilot:")
    if "AZURE" in region:
        st.error(
            """
        NOTE: OpsCenter is in preview release on Azure and some functionality is currently unavailable.
        The Sundeck team is working on updating the app to fully support Azure.
        If you have any questions reach out to support@sundeck.io."""
        )
    with st.sidebar:
        cols = st.columns([1, 20, 1])
        with cols[1]:
            # st.title('Sundeck OpsCenter')
            image_svg("opscenter_logo.svg")
            pass

import streamlit as st
from packaging import version

def set_font_lato():
    # Custom fonts disabled
    pass

def container_object_with_height_if_possible(height: int):
    """
    Deal with annoying issue that snowflake streamlit uses a very old version of streamlit which doesn't have height 
    parameter for container.
    
    Args:
        height(int):
            Height in pixels to set for the container
    Returns:
        Streamlit container object with height set if supported, otherwise a standard container.
    """
    streamlit_version = st.__version__
    if version.parse(streamlit_version) >= version.parse("1.30"):  # use locally 
        return st.container(height=height)
    else:
        return st.container() # height support added years ago but snowflake on streamlit using a 
        # stone age version (1.22)
def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    cond_match = (
      (line_dict['if1'] != '') and (20 < int(line_dict["if1"]) < 40)
    ) 
    return True if cond_match else False

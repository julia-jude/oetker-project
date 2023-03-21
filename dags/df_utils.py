from pyspark.sql.functions import *

def cleanse_string(col):
    """
    removes any non-alphabetic characters from the string column , trim the whitespaces and
     convert the result to uppercase.

    Args:
        col:passes the string column to be cleaned

    Returns:
        column containing the cleaned string
    """
    return upper(trim(regexp_replace(col, "[^a-zA-Z\s]", "")))


def cleanse_email(email_col):
    """
    Cleanse the email column by removing whitespaces, any paarantehesis,
    or any non-alphanumeric characters except for "@" and "." and then passes the cleansed email column
    Args:
        email_col:email column to be cleansed
    Returns:
        cleansed email column
    """
    # remove any leading/trailing whitespace
    cleaned_col = trim(email_col)
    cleaned_col = trim(regexp_replace(cleaned_col, "^\s+", ''))
    # convert to lower-case
    cleaned_col=lower(cleaned_col)
    # remove any parentheses around the email address
    cleaned_col = regexp_replace(cleaned_col, r'^\(|\)$', '')
    # remove any non-alphanumeric characters except for "@" and "."
    cleaned_col = regexp_replace(cleaned_col, r'[^\w@\.]', '')
    return cleaned_col


def cleanse_id(col):
    """
    Cleanse the id column by removing whitespaces and
    any non-numeric characters and then passes the cleansed id column
    Args:
        col:id column to be cleansed
    Returns:
        cleansed id column
    """
    return trim(regexp_replace(col, r"[^\d]+", ''))


def check_ip_format(ip_col):
    """
    checks the format for ip-address matches the ipv4 format or not
    Args:
        ip_col: ip address column to be checked for its format
    Returns:
        returns the extracted ip-addresses which matches the ipv4 format
    """
    # regular expression pattern to match an IPv4 address
    ipv4_format = r'^([0-9]{1,3}\.){3}[0-9]{1,3}$'

    # extract the ip address from the column using the regular expression pattern
    extracted_ip = regexp_extract((ip_col), ipv4_format, 0)

    return extracted_ip


def cleanse_date(date_col):
    """
    Cleanse the date column by converting the format from "DD/MM/YYYY" to "YYYY-MM-DD"
    Args:
        date_col:date column to be cleansed
    Returns:
        cleansed date column
    """
    return regexp_replace(date_col, r'(\d{2})/(\d{2})/(\d{4})', '$3-$2-$1')
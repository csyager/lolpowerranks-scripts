import json

def visualize_mapping_data_json():
    """ Visualize the mapping data json file
        produces graphs showing wins aggregated by 
        @return: None
    """
    with open("esports-data/mapping_data.json", "r") as json_file:
        mappings_data = json.load(json_file)

    with open("mapping.html", "w") as html_file:
        html_file.write("<html><body><h1>Mapping Data</h1>")
        html_file.write("<table><tr><th>Platform Game ID</th><th>Esports Game ID</th></tr>")
        for mapping in mappings_data:
            html_file.write(f"<tr><td>{mapping['platformGameId']}</td><td>{mapping['esportsGameId']}</td></tr>")
        html_file.write("</table></body></html>")

def tournament_match_strategy_to_pie_graph():
    """ Parse tournaments.json file and

    """



if __name__ == '__main__':
    visualize_mapping_data_json()
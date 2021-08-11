from pyspark.sql.types import StructType
import json

class spark_utils:

    def get_json_schema(self, file_path):
        f = open(file_path, )
        data = json.load(f)
        new_schema = StructType.fromJson(data)
        return new_schema

    # UDF to calculate time in minutes
    def calculate_time(self, time_column):
        if time_column == "PT" or time_column == "":
            return 0
        else:
            trim_column = time_column.replace("PT", "")
            hours = trim_column.split("H")
            minutes = 0
            try:
                if len(hours) == 2 and hours[1] != "":
                    minutes = int(hours[1].split("M")[0]) + int(hours[0]) * 60
                elif len(hours) == 2 and hours[1] == "":
                    minutes = int(hours[0]) * 60
                elif len(hours) == 1:
                    minutes = int(int(hours[0].split("M")[0]))
                return minutes
            except:
                print("Error occurred while calculating time, input - ", time_column)
                return 0

    # UDF to calculate complexity of recipes
    def calculate_complexity(self, total_time):
        if total_time == "" or int(total_time) == 0:
            return "Invalid case"
        else:
            if int(total_time) < 30:
                return "easy"
            elif int(total_time) >= 30 and int(total_time) <= 60:
                return "medium"
            elif int(total_time) > 60:
                return "difficult"




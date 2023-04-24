from jsonschema import validate

class DBSyncService():

    old_name = "core"
    new_name = "api"

    schema = {
        "type": "array",
        "items":{
            "type" : "object",
            "properties" : {
                old_name : {"type" : "object"},
                new_name : {"type" : "object"},
            },
        }
    }

    def getModel(self, table_name, type):
        _map = {
            self.old_name: {
                "custom_user": ("auth", "User"),
            },
            self.new_name: {
                "custom_user": ("auth", "User"),
            }
        }

    def convertJSONtoRelation(json): # old > new
        validate(instance=json, schema=schema)
        change = json

        for change in changes:
            old = change[self.old_name]
            new = change[self.new_name]
            table_name = change["table_name"]

            for (index, old_field) in old:
                new_field = new[index]

                if "table_name" in old_field
                    ...
                else:
                    app_name, model_name = self.getModel(table_name, self.old_name)
                    model = apps.get_model(app_label=app_name, model_name=model_name)
                    model.objects.filter(
                        {
                            json["filter"][index]: 
                        }
                    )

    #conversion_pattern = { "from": None, "to": None}
    def (self, table_conversion, field_conversion, filter_conversion)
        table_conversion["from"].filter(filter_conversion).(field["from"])
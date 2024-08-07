import pandas as pd
import pyodbc
from datetime import datetime
import tkinter as tk
from tkinter import messagebox, StringVar, filedialog
import Connection

class ELTUpdater:
    def __init__(self, connection_string_pb, connection_string_we):
        self.cnxn_pb = pyodbc.connect(Connection.connection(connection_string_pb))
        self.cnxn_we = pyodbc.connect(Connection.connection(connection_string_we))
        self.cursor_pb = self.cnxn_pb.cursor()
        self.cursor_we = self.cnxn_we.cursor()
        self.cursors = [self.cursor_pb, self.cursor_we]
        self.fabric = []
        self.color = []
        self.vendor = []
        self.elt = []

    def set_inputs(self, fabric, color, vendor, elt=None):
        self.fabric = [x.strip().title() for x in fabric.split(",")]
        self.color = [x.strip().title() for x in color.split(",")]
        self.vendor = [int(x.strip()) for x in vendor.split()]
        if elt:
            self.elt = [int(x.strip()) for x in elt.split()]

    def update_entry(self, fabric, color, vendor, lead_time):
        update_query = f"""
        UPDATE [tbl Data Entry]
        SET EX_Lead_Time = {lead_time}
        WHERE Fabric = '{fabric}' AND Color = '{color}' AND [Vendor #] = '{vendor}'
        """
        for cursor in self.cursors:
            cursor.execute(update_query)
            cursor.connection.commit()

    def update_entry_conditionally(self, fabric, color, vendor, lead_time):
        update_query = f"""
        UPDATE [tbl Data Entry]
        SET EX_Lead_Time = {lead_time}
        WHERE (Fabric = '{fabric}' AND Color = '{color}' AND [Vendor #] = '{vendor}' AND [Collection] = 'Fabric By The Yard')
        OR (Fabric = '{fabric}' AND Color = '{color}' AND [Vendor #] = '{vendor}' AND [Slipcover/Upholstered/Set] = 'Slipcover')
        """
        for cursor in self.cursors:
            cursor.execute(update_query)
            cursor.connection.commit()

    def nla_query(self, fabric, color, vendor):
        current_date = datetime.now().strftime("%Y-%m-%d")
        nla_update_query = f"""
        UPDATE [tbl Data Entry]
        SET EX_Lead_Time = 'NLA', 
        [Season Code] = CASE
            WHEN [Dept #] = 217 THEN 'INACTIVE BR'
            WHEN [Dept #] = 223 THEN 'INACTIVE DINING'
            ELSE 'INACTIVE'
        END,
        [Batch_Notes] = 'NLA per Jackie Welland {current_date}' 
        WHERE Fabric = '{fabric}' AND Color = '{color}' AND [Vendor #] = '{vendor}'
        """
        for cursor in self.cursors:
            cursor.execute(nla_update_query)
            cursor.connection.commit()

    def fetch_data(self, fabric, color, vendor):
        query = f"""
        SELECT [Parent Sku], Sku, [Dummy Sku], Description, [Season Code], [Season Launch], [Season Drop], [Dept #], [Sku Status], Collection, [Vendor #], COO, [Country Of Origin], Frame, Tier, Fabric, Color, Size, Finish,
        [Down or Poly], [Slipcover/Upholstered/Set], [First Cost], [ELC calc], [Catalog Retail Price], Retail_CAN, Length, Height, Width, Weight, EX_Lead_Time, [Ship Mode], COorNew, Contract, Greenguard, [Group ID], [Set Sku?]
        FROM dbo.[tbl Data Entry]
        WHERE Fabric = '{fabric}' AND Color = '{color}' AND (NOT ([Season Code] LIKE N'%INACTIVE%')) AND [Vendor #] = '{vendor}' AND Sku <> 0 AND (NOT ([Season Code] LIKE N'%COST SHELL%'))
        """
        df = pd.DataFrame()
        for cursor in self.cursors:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                columns = [column[0] for column in cursor.description]
                mod_result = pd.DataFrame.from_records(result, columns=columns)
                mod_result['Collection'] = mod_result['Collection'].str.title()
                mod_result['Fabric'] = mod_result['Fabric'].str.title()
                mod_result['Color'] = mod_result['Color'].str.title()
                df = pd.concat([df, mod_result], ignore_index=True)
        return df

    def update_and_fetch(self, df, file_path, file_name):
        if file_name != "NLA":
            previous_value = (None, None, None)
            for f, c, v, e in zip(self.fabric, self.color, self.vendor, self.elt):
                if (f, c, v) == previous_value:
                    self.update_entry_conditionally(f, c, v, e)
                    mask = (df['Fabric'] == f) & (df['Color'] == c) & (df['Vendor #'] == v)
                    df.loc[mask, 'EX_Lead_Time'] = e
                else:
                    self.update_entry(f, c, v, e)
                previous_value = (f, c, v)
                new_data = self.fetch_data(f, c, v)
                df = pd.concat([df, new_data], ignore_index=True)
            
            df.to_csv(f'{file_path}/{file_name}.csv', index=False)
        else:
            for f, c, v in zip(self.fabric, self.color, self.vendor):
                new_data = self.fetch_data(f, c, v)
                df = pd.concat([df, new_data], ignore_index=True)
                df['EX_Lead_Time'] = 'NLA'
                df['Sku Status'] = 'Inactive'
                df.loc[df['Dept #'] == 223, 'Season Code'] = 'INACTIVE DINING'
                df.loc[df['Dept #'] == 217, 'Season Code'] = 'INACTIVE BR'
                df.loc[(df['Dept #'] != 223) & (df['Dept #'] != 217), 'Season Code'] = 'INACTIVE'
                self.nla_query(f, c, v)


def select_folder():
    folder_selected = filedialog.askdirectory()
    file_path_var.set(folder_selected)

def on_file_name_change(*args):
    if file_name_var.get() == "NLA":
        elt_label.grid_remove()
        elt_entry.grid_remove()
    else:
        elt_label.grid()
        elt_entry.grid()

def execute_script():
    fabric = fabric_entry.get()
    color = color_entry.get()
    vendor = vendor_entry.get()
    elt = elt_entry.get()
    file_name = file_name_var.get()
    file_path = file_path_var.get()

    db_manager.set_inputs(fabric, color, vendor, elt if file_name != "NLA" else None)
    df = pd.DataFrame() 
    db_manager.update_and_fetch(df, file_path, file_name)

    messagebox.showinfo("Success", "Script executed successfully!")


db_manager = ELTUpdater('PB_Upholstery', 'WE_Upholstery')


root = tk.Tk()
root.title("ELT Updater")


root.columnconfigure(1, weight=1)
for i in range(7):
    root.rowconfigure(i, weight=1)


tk.Label(root, text="Fabrics:").grid(row=0, column=0, sticky="e")
fabric_entry = tk.Entry(root)
fabric_entry.grid(row=0, column=1, sticky="ew")

tk.Label(root, text="Colors:").grid(row=1, column=0, sticky="e")
color_entry = tk.Entry(root)
color_entry.grid(row=1, column=1, sticky="ew")

tk.Label(root, text="Vendors:").grid(row=2, column=0, sticky="e")
vendor_entry = tk.Entry(root)
vendor_entry.grid(row=2, column=1, sticky="ew")


elt_label = tk.Label(root, text="ELT:")
elt_label.grid(row=3, column=0, sticky="e")
elt_entry = tk.Entry(root)
elt_entry.grid(row=3, column=1, sticky="ew")


file_name_var = StringVar(root)
file_name_var.set("Updates")  
file_name_var.trace_add("write", on_file_name_change)

tk.Label(root, text="File Name:").grid(row=6, column=2, sticky="e")
file_name_menu = tk.OptionMenu(root, file_name_var, "Updates", "Removes", "NLA")
file_name_menu.grid(row=6, column=3, sticky="ew")


file_path_var = StringVar(root)
tk.Label(root, text="File Path:").grid(row=4, column=0, sticky="e")
file_path_entry = tk.Entry(root, textvariable=file_path_var)
file_path_entry.grid(row=4, column=1, sticky="ew")
tk.Button(root, text="Browse", command=select_folder).grid(row=4, column=2)


execute_button = tk.Button(root, text="Execute", command=execute_script)
execute_button.grid(row=6, column=0, columnspan=3, pady=10)


root.mainloop()

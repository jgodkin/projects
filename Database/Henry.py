"""James Godkin
Henry creates the GUI and records the actions of the drop down menus to request the information from HenryInterFaceClasses
and HenryDOA"""

import tkinter as tk
from tkinter import ttk
import HenryInterfaceClasses as hic
from HenryDAO import henryDAO

"""Search By Author Functions"""
def comCallback2(event):
    # get will get its value - note that this is always a string
    selIndex2 = event.widget.current()

    # Label Price
    lab3 = ttk.Label(tab1)
    lab3.grid(column=3, row=4)
    lab3['text'] = "Price:"
    labPrice = ttk.Label(tab1)
    labPrice.grid(column=4, row=4)
    labPrice['text'] = '$' + str(author.getPrice(selIndex2))

    #Store Info
    listOfStoreInfo = author.getInfo(selIndex2)
    for i in tree1.get_children():  # Remove any old values in tree list
        tree1.delete(i)

    for row in listOfStoreInfo:
        tree1.insert("", "end", values=[row[0], row[1]])

def comCallback(event):
    # get will get its value - note that this is always a string
    selIndex = event.widget.current()

    # Label Books
    lab2 = ttk.Label(tab1)
    lab2.grid(column=3, row=0)
    lab2['text'] = "Books"

    # Combobox Books
    com2 = ttk.Combobox(tab1, width=20, state="readonly")
    com2.grid(column=3, row=2)
    bookList = author.getBooks(selIndex)
    com2['values'] = bookList
    com2.current(0)
    com2.bind("<<ComboboxSelected>>", comCallback2)
    #com2.bind("<<ComboboxSelected>>", lambda event: comCallback2(event, bookList)) send argument with event

    # Label Price
    lab3 = ttk.Label(tab1)
    lab3.grid(column=3, row=4)
    lab3['text'] = "Price:"
    labPrice = ttk.Label(tab1)
    labPrice.grid(column=4, row=4)
    labPrice['text'] = '$' + str(0)

    # Store Info
    for i in tree1.get_children():  # Remove any old values in tree list
        tree1.delete(i)
"""End of Search By Author Functions"""

"""Search By Category Functions"""
def comCallback4(event):
    # get will get its value - note that this is always a string
    selIndex4 = event.widget.current()

    # Label Price
    lab3 = ttk.Label(tab2)
    lab3.grid(column=3, row=4)
    lab3['text'] = "Price:"
    labPrice = ttk.Label(tab2)
    labPrice.grid(column=4, row=4)
    labPrice['text'] = '$' + str(category.getPrice(selIndex4))

    #Store Info
    listOfStoreInfo = category.getInfo(selIndex4)
    for i in tree2.get_children():  # Remove any old values in tree list
        tree2.delete(i)

    for row in listOfStoreInfo:
        tree2.insert("", "end", values=[row[0], row[1]])

def comCallback3(event):
    # get will get its value - note that this is always a string
    selIndex3 = event.widget.current()

    # Label Books
    lab2 = ttk.Label(tab2)
    lab2.grid(column=3, row=0)
    lab2['text'] = "Books"

    # Combobox Books
    com2 = ttk.Combobox(tab2, width=20, state="readonly")
    com2.grid(column=3, row=2)
    bookList = category.getBooks(selIndex3)
    com2['values'] = bookList
    com2.current(0)
    com2.bind("<<ComboboxSelected>>", comCallback4)
    #com2.bind("<<ComboboxSelected>>", lambda event: comCallback2(event, bookList)) send argument with event

    # Label Price
    lab3 = ttk.Label(tab2)
    lab3.grid(column=3, row=4)
    lab3['text'] = "Price:"
    labPrice = ttk.Label(tab2)
    labPrice.grid(column=4, row=4)
    labPrice['text'] = '$' + str(0)

    # Store Info
    for i in tree2.get_children():  # Remove any old values in tree list
        tree2.delete(i)
"""End of Search By Category Functions"""

"""Search By Publisher Functions"""
def comCallback6(event):
    # get will get its value - note that this is always a string
    selIndex6 = event.widget.current()

    # Label Price
    lab3 = ttk.Label(tab3)
    lab3.grid(column=3, row=4)
    lab3['text'] = "Price:"
    labPrice = ttk.Label(tab3)
    labPrice.grid(column=4, row=4)
    labPrice['text'] = '$' + str(publisher.getPrice(selIndex6))

    #Store Info
    listOfStoreInfo = publisher.getInfo(selIndex6)
    for i in tree3.get_children():  # Remove any old values in tree list
        tree3.delete(i)

    for row in listOfStoreInfo:
        tree3.insert("", "end", values=[row[0], row[1]])

def comCallback5(event):
    # get will get its value - note that this is always a string
    selIndex5 = event.widget.current()

    # Label Books
    lab2 = ttk.Label(tab3)
    lab2.grid(column=3, row=0)
    lab2['text'] = "Books"

    # Combobox Books
    com2 = ttk.Combobox(tab3, width=20, state="readonly")
    com2.grid(column=3, row=2)
    bookList = publisher.getBooks(selIndex5)
    com2['values'] = bookList
    com2.current(0)
    com2.bind("<<ComboboxSelected>>", comCallback6)
    #com2.bind("<<ComboboxSelected>>", lambda event: comCallback2(event, bookList)) send argument with event

    # Label Price
    lab3 = ttk.Label(tab3)
    lab3.grid(column=3, row=4)
    lab3['text'] = "Price:"
    labPrice = ttk.Label(tab3)
    labPrice.grid(column=4, row=4)
    labPrice['text'] = '$' + str(0)

    # Store Info
    for i in tree3.get_children():  # Remove any old values in tree list
        tree3.delete(i)
"""End of Search By Publisher Functions"""

# Main window
root = tk.Tk()
root.title("Henry Online Bookstore Operator")
root.geometry('800x400')

# Tab control
tabControl = ttk.Notebook(root)
tab1 = ttk.Frame(tabControl)  # tab1 and tab2 are tab window names
tab2 = ttk.Frame(tabControl)
tab3 = ttk.Frame(tabControl)
tabControl.add(tab1, text='Search By Author')
tabControl.add(tab2, text='Search By Category')
tabControl.add(tab3, text='Search By Publisher')
tabControl.pack(expand=1, fill="both")

'''Search By Author Tab'''
# Label Choose Your Author
lab1 = ttk.Label(tab1)
lab1.grid(column=1, row=0)
lab1['text'] = "Choose Your Author"

# Combobox Choose Your Author
com1 = ttk.Combobox(tab1, width=20, state="readonly")
com1.grid(column=1, row=2)
author = hic.author(henryDAO().getAuthorName())
authorList = author.authorList()
com1['values'] = authorList
com1.current(0)
com1.bind("<<ComboboxSelected>>", comCallback)

# Label Blank
labBlank1 = ttk.Label(tab1)
labBlank1.grid(column=1, row=3)
labBlank1['text'] = ""
labBlank2 = ttk.Label(tab1)
labBlank2.grid(column=2, row=4)
labBlank2['text'] = "      "

# Treeview set up
tree1 = ttk.Treeview(tab1, columns=('Store Name', 'In Stock'), show='headings')
tree1.heading('Store Name', text='Store Name')
tree1.heading('In Stock', text='In Stock')
tree1.grid(column=1, row=4)

for i in tree1.get_children():  # Remove any old values in tree list
    tree1.delete(i)

'''Search By Category Tab'''
# Label Choose Your Category
lab1 = ttk.Label(tab2)
lab1.grid(column=1, row=0)
lab1['text'] = "Choose A Category"

# Combobox Choose Your Category
com1 = ttk.Combobox(tab2, width=20, state="readonly")
com1.grid(column=1, row=2)
category = hic.category(henryDAO().getCategory())
categoryList = category.getCategoryList()
com1['values'] = categoryList
com1.current(0)
com1.bind("<<ComboboxSelected>>", comCallback3)

# Label Blank
labBlank1 = ttk.Label(tab2)
labBlank1.grid(column=1, row=3)
labBlank1['text'] = ""
labBlank2 = ttk.Label(tab2)
labBlank2.grid(column=2, row=4)
labBlank2['text'] = "      "

# Treeview set up
tree2 = ttk.Treeview(tab2, columns=('Store Name', 'In Stock'), show='headings')
tree2.heading('Store Name', text='Store Name')
tree2.heading('In Stock', text='In Stock')
tree2.grid(column=1, row=4)

for i in tree2.get_children():  # Remove any old values in tree list
    tree2.delete(i)

'''Search By Publisher'''
# Label Choose Your Publisher
lab1 = ttk.Label(tab3)
lab1.grid(column=1, row=0)
lab1['text'] = "Find a Publisher"

# Combobox Choose Your Publisher
com1 = ttk.Combobox(tab3, width=20, state="readonly")
com1.grid(column=1, row=2)
publisher = hic.publisher(henryDAO().getPublisherName())
publisherNames = publisher.getPublisherList()
com1['values'] = publisherNames
com1.current(0)
com1.bind("<<ComboboxSelected>>", comCallback5)

# Label Blank
labBlank1 = ttk.Label(tab3)
labBlank1.grid(column=1, row=3)
labBlank1['text'] = ""
labBlank2 = ttk.Label(tab3)
labBlank2.grid(column=2, row=4)
labBlank2['text'] = "      "

# Treeview set up
tree3 = ttk.Treeview(tab3, columns=('Store Name', 'In Stock'), show='headings')
tree3.heading('Store Name', text='Store Name')
tree3.heading('In Stock', text='In Stock')
tree3.grid(column=1, row=4)

for i in tree3.get_children():  # Remove any old values in tree list
    tree3.delete(i)

root.mainloop()
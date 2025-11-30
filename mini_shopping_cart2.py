import sqlite3
import sys

conn = sqlite3.connect("store.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    price INTEGER,
    stock INTEGER
)
""")

cur.execute("SELECT COUNT(*) FROM items")
if cur.fetchone()[0] == 0:
    cur.executemany("INSERT INTO items (name, price, stock) VALUES (?, ?, ?)", [
        ("Apple", 20, 10),
        ("Milk", 60, 5),
        ("Bread", 40, 8)
    ])
    conn.commit()

cur.execute("SELECT name, price, stock FROM items")
rows = cur.fetchall()

items = [r[0] for r in rows]
prices = [r[1] for r in rows]
stock = [r[2] for r in rows]

cart_items = []
cart_qty = []
cart_price = []

print("---- WELCOME TO STORE ----")
print("Available items:")

for i in range(len(items)):
    print(i+1, ".", items[i], "- Rs", prices[i], "- Qty:", stock[i])

print("\nEnter the item number to buy. Type 0 to finish.\n")

while True:
    choice = int(input("Item number: "))

    if choice == 0:
        break

    if choice < 1 or choice > len(items):
        print("Invalid item number!")
        continue

    qty = int(input("Enter quantity: "))

    if qty > stock[choice-1]:
        print("Sorry, not enough stock!")
        continue

    cart_items.append(items[choice-1])
    cart_qty.append(qty)
    cart_price.append(prices[choice-1] * qty)

    stock[choice-1] -= qty

    print("Added to cart!")

if len(cart_items) == 0:
    print("Your cart is empty. Exiting.")
    sys.exit()

for i in range(len(items)):
    cur.execute("UPDATE items SET stock = ? WHERE name = ?", (stock[i], items[i]))
conn.commit()

print("\nEnter customer details:")
name = input("Name: ")
address = input("Address: ")
distance = int(input("Distance from store (km): "))

if distance <= 15:
    delivery = 50
elif distance <= 30:
    delivery = 100
else:
    print("Delivery not available! Pickup only.")
    delivery = 0

print("\n----- FINAL BILL -----")
print("Customer:", name)
print("Address:", address)

total = 0
print("\nItems purchased:")
for i in range(len(cart_items)):
    print(cart_items[i], "x", cart_qty[i], "= Rs", cart_price[i])
    total += cart_price[i]

print("\nDelivery Charges: Rs", delivery)
print("Total Amount: Rs", total + delivery)
print("----------------------")
print("Thank you for shopping!")

conn.close()

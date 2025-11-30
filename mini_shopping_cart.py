items = ["Apple", "Milk", "Bread"]
prices = [20, 60, 40]
stock = [10, 5, 8]

cart_items = []
cart_qty = []
cart_price = []

print("---- WELCOME TO STORE ----")
print("Available items:")

for i in range(len(items)):
    print(i+1, ".", items[i], "- Rs", prices[i], "- Qty:", stock[i])

print("\nEnter the item number to buy. Type 0 to finish.\n")

# ---- CART SELECTION ----
while True:
    choice = int(input("Item number: "))

    if choice == 0:
        break

    if choice < 1 or choice > len(items):
        print("Invalid item number!")
        continue

    qty = int(input("Enter quantity: "))

    # Check stock
    if qty > stock[choice-1]:
        print("Sorry, not enough stock!")
        continue

    # Add to cart
    cart_items.append(items[choice-1])
    cart_qty.append(qty)
    cart_price.append(prices[choice-1] * qty)

    # Reduce stock
    stock[choice-1] -= qty

    print("Added to cart!")

# If cart empty
if len(cart_items) == 0:
    print("Your cart is empty. Exiting.")
    exit()

# ---- CUSTOMER DETAILS ----
print("\nEnter customer details:")
name = input("Name: ")
address = input("Address: ")
distance = int(input("Distance from store (km): "))

# ---- DELIVERY CHARGES ----
if distance <= 15:
    delivery = 50
elif distance <= 30:
    delivery = 100
else:
    print("Delivery not available! Pickup only.")
    delivery = 0

# ---- BILL ----
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

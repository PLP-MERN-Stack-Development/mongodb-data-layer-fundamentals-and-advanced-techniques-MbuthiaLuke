## Error: Cannot find module 'mongodb'

# MongoDB Setup Instructions for Week 1

If you encounter this error when running your JavaScript files:

```
Error: Cannot find module 'mongodb'
```

It means the MongoDB Node.js driver is not installed in your project. Follow these steps to fix it:

## Step 1: Make sure Node.js is installed

You can check if Node.js is installed by running:

```bash
node --version
```

If not installed, download it from [nodejs.org](https://nodejs.org/).

## Step 2: Initialize your project (if not done already)

Navigate to your assignment directory and run:

```bash
npm init -y
```

This creates a `package.json` file to manage your project dependencies.

## Step 3: Install the MongoDB driver

Run this command in your assignment directory:

```bash
npm install mongodb
```

## Step 4: Try running your scripts again

After installing the MongoDB driver, you should be able to run your JavaScript files:

```bash
node insert_books.js
```

## Alternative: Using MongoDB Shell

If you prefer not to use Node.js for this assignment, you can use the MongoDB Shell (mongosh) directly:

1. Start your MongoDB server
2. Run your script with MongoDB Shell:
   ```bash
   mongosh --file your_script.js
   ```

Example MongoDB Shell scripts are provided in the examples directory.

## Getting Help

If you continue to experience issues, please:

1. Make sure MongoDB is installed and running on your system
2. Check that you're in the correct directory when installing packages
3. Verify your MongoDB connection string in your code

For more information, refer to the [MongoDB Node.js Driver documentation](https://www.mongodb.com/docs/drivers/node/current/).

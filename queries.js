const { MongoClient } =  require ("mongodb");

// connection uri
const uri =  "mongodb://localhost:27017";
const dbName = "plp_bookstore";
const collectionName = "books";

async function runAllQueries() {
    const client = new MongoClient(uri);

    try {
        await client.connect();
        console.log("Connected to MongoDB server\n");

        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        // TASK 2: Basic CRUD Operations
        console.log("=== Task 2: Basic CRUD opperations ===\n");

        // 1. Find all books in a specific genre
        console.log("1. Find all books in a specific genre")
        const fictionBooks = await collection.find({ genre: "Fiction" }).toArray();
        fictionBooks.forEach(book => {
            console.log(`   - "${book.title}" by ${book.author}`);
        });

        // 2. Find books published after a certain year
        console.log('\n2. Books published after 1950:');
        const recentBooks = await collection.find({ published_year: { $gt: 1950 } }).toArray();
        recentBooks.forEach(book => {
            console.log(`   - "${book.title}" (${book.published_year})`);
        });
        
        // 3. Find books by a specific author
        console.log('\n3. Books by George Orwell:');
        const orwellBooks = await collection.find({ author: "George Orwell" }).toArray();
        orwellBooks.forEach(book => {
            console.log(`   - "${book.title}" (${book.published_year})`);
        });
        // 4. Update the price of a specific book
        console.log('\n4. Updating price of "The Hobbit":');
        const updateResult = await collection.updateOne(
            { title: "The Hobbit" },
            { $set: {price: 15.99} }
        );
        console.log(`   updated ${updateResult.modifiedCount} document(s)`);
        
        // Verify the update
        const updateBook = await collection.findOne({ title: "The Hobbit" });
        console.log(`  New price: ${updateBook.price}`);

        // 5. Delete a book by its title
        console.log('\n5. Deleting "Moby Dick":');
        const deleteResault = await collection.deleteOne({ title: "Moby Dick" });
        console.log (`   Deleted ${deleteResault.deletedCount} document(s)`)

        // TASK 3: Advanced Queries
    
        console.log('\n=== TASK 3: Advanced Queries ===\n');

        // 1. Books that are both in stock AND published after 2000
        console.log('1. In-stock books published after 2000:');
        const inStockRecent = await collection.find({
            in_stock: true,
            published_year: { $gt: 2000}
        }).toArray();
        console.log (`   Found in stock ${inStockRecent.length} books`)

        // 2. Projection - only title, author, price
        console.log('\n2. Books with projection (title, author, price only):');
        const projectedBooks = await collection.find(
            { genre: "Fiction" },
            { projection: { title: 1, author: 1, price: 1, _id: 0} }
        ).limit(3).toArray();
        console.log(projectedBooks);

        // 3. Sorting by price (ascending and descending)
        console.log('\n3. Books sorted by price (ascending):');
        const sortedAsc = await collection.find({})
            .sort({ price: 1})
            .limit(3)
            .project({ title: 1, price: 1, _id: 0})
            .toArray();
        sortedAsc.forEach(book => {
            console.log(`   - "${book.title}": ${book.price}`);
        });

        console.log('\n4. Books sorted by price (descending):');
        const sortedDesc = await collection.find({})
            .sort({ price: -1})
            .limit(3)
            .project({ title: 1, price: 1, _id: 0})
            .toArray();
        sortedDesc.forEach(book => {
            console.log(`   - "${book.title}": ${book.price}`);
        });

        // 4. Pagination with limit and skip
        console.log('\n5. Pagination - Page 1 (5 books):');
        const page1 = await collection.find({})
            .sort({ title: 1 })
            .limit(5)
            .project({ title: 1, author: 1, _id: 0 })
            .toArray();
        page1.forEach((book, index) => {
        console.log(`   ${index + 1}. "${book.title}" by ${book.author}`);
        });

        console.log('\n6. Pagination - Page 2 (next 5 books):');
        const page2 = await collection.find({})
            .sort({ title: 1 })
            .skip(5)
            .limit(5)
            .project({ title: 1, author: 1, _id: 0 })
            .toArray();
        page2.forEach((book, index) => {
        console.log(`   ${index + 1}. "${book.title}" by ${book.author}`);
        });

        // TASK 4: Aggregation Pipeline
    
        console.log('\n=== TASK 4: Aggregation Pipeline ===\n');

        // 1. Average price by genre
        console.log('1. Average price by genre:');
        const avgPriceByGenre = await collection.aggregate([
            {
                $group: {
                    _id: "$genre",
                    avaragePrice: { $avg: "$price" },
                    bookCount: { $sum: 1 }
                }
            },
        {
            $sort: { averagePrice: -1 }
        }
        ]).toArray();

        avgPriceByGenre.forEach(genre => {
            console.log(`   - ${genre._id}: $${genre.avaragePrice.toFixed(2)} (${genre.bookCount} books)`);
        });

        // 2. Author with most books
        console.log('\n2. Author with most books:');
        const authorMostBooks = await collection.aggregate ([
            {
                $group: {
                    _id: "$author",
                    bookCount: { $sum: 1 }
                }
            },
            {
                $sort: { bookCount: -1 }
            },
            {
                $limit: 3
            }
        ]).toArray();

        authorMostBooks.forEach((author, index) => {
            console.log(`   ${index +1}.${author._id}: ${author.bookCount} books`);
        });

        // 3. Books by publication decade
        console.log('\n3. Books by publication decade:');
        const booksByDecade = await collection.aggregate([
            {
                $project: {
                    title: 1,
                    published_year: 1,
                    decade: {
                        $subtract: [
                            "$published_year",
                            { $mod: ["$published_year", 10]}
                        ]
                    }
                }
            },
            {
                $group: {
                    _id: "$decade",
                    bookCount: { $sum: 1 },
                    books: { $push: "$title" }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ]).toArray();

        booksByDecade.forEach(decade => {
            console.log(`   -${decade._id}s: ${decade.bookCount} books`);
        });

        // TASK 5: Indexing
    
        console.log('\n=== TASK 5: Indexing ===\n');

        // 1. Create index on title field
        console.log('1.  Creating index on title field...');
        await collection.createIndex({ title: 1 });
        console.log("   Index created in title field.");

        // 2. Create compound index on author and published_year
        console.log('\n2. Creating compound index on author and published_year...');
        await collection.createIndex({ author: 1, published_year: 1 });
        console.log("   compound index created on author and published_year.");

        // 3. Use explain() to show performance improvement
        console.log('\n3.  Performance analysis with explain():');
        
        // Without index (simulate by hinting no index)
        console.log('   Query without index:');
        const withoutIndex = await collection.find({ title: "1984" }).explain("executionStats");
        console.log(`   Documents examined: ${withoutIndex.executionStats.totalDocsExamined}`);
        console.log(`   Execution Time (ms): ${withoutIndex.executionStats.executionTimeMillis}`);
        

        console.log('   Query with index:');
        const withIndex = await collection.find({ title: "1984" })
            .hint({ title: 1 })
            .explain("executionStats");
        console.log(`   Documents examined: ${withIndex.executionStats.totalDocsExamined}`);
        console.log(`   Execution Time (ms): ${withIndex.executionStats.executionTimeMillis}`);

        // Show all indexes
        console.log('\n4. Current indexes on books collection:');
        const indexes = await collection.indexes();
        indexes.forEach((index, i) => {
            console.log(`   ${i + 1}. ${JSON.stringify(index.key)}`);

        });
    } catch (error) {
        console.error("Error: ", error);
    } finally {
        await client.close();
        console.log("\n Connection closed");
    }
}

// Run all queries
runAllQueries().catch(console.error);
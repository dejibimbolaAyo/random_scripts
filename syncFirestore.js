require('dotenv').config();
const Random = require("random-id");
let MongoClient = require('mongodb').MongoClient;
const _ = require("underscore");
const SimpleSchema = require("simpl-schema");
let admin = require("firebase-admin");

const dburl = process.env.MONGO_DBURL;
let db

SimpleSchema.extendOptions(['index', 'decimal', 'unique']);

const AreaBasketVariant = new SimpleSchema({
  _id: {
    type: String,
    optional: true
  },
  brandId: {
    type: String,
    optional: true
  },
  brandName: {
    type: String,
    optional: true
  },
  category: {
    type: String,
    optional: true
  },
  categoryGroupId: {
    type: String,
    optional: true
  },
  categoryId: {
    type: String,
    optional: true
  },
  code: {
    type: String
  },
  currency: {
    type: Object,
    optional: true
  },
  "currency.iso": {
    type: String
  },
  "currency.symbol": {
    type: String,
    optional: true
  },
  dateAdded: {
    type: Date,
    optional: true
  },
  description: {
    type: String,
    optional: true
  },
  enabled: {
    type: Boolean,
    defaultValue: true
  },
  isFeatured: {
    type: Boolean,
    optional: true,
    defaultValue: false
  },
  name: {
    type: String
  },
  hexCode: {
    type: String,
    index: 1
  },
  price: {
    type: Number,
    decimal: true
  },
  productCreatedAt: {
    type: Date,
    optional: true
  },
  productId: {
    type: String
  },
  productName: {
    type: String
  },
  subUnit: {
    type: Object,
    blackbox: true
  },
  "subUnit.syncedAt": {
    type: Date,
    optional: true,
    autoValue: function () {
      return new Date()
    }
  },
  tags: {
    type: Array,
    optional: true
  },
  "tags.$": {
    type: String
  },
  unitDescription: {
    type: Array,
    optional: true
  },
  "unitDescription.$": {
    type: Object,
    blackbox: true
  },
  updatedAt: {
    type: Date,
    optional: true
  },
  variantCreatedAt: {
    type: Date,
    optional: true
  },
  variantId: {
    type: String
  },
  variantHexCode: {
    type: String,
    optional: true,
    unique: true,
    autoValue: function () {
      const variantId = this.siblingField("variantId").value;
      const hexCode = this.siblingField("hexCode").value
      if (variantId && hexCode) {
        return `${variantId}_${hexCode}`;
      }
    }
  },
})

const initFirebase = async () => {
  let databaseURL = process.env.FIREBASE_DBURL;
  if (!databaseURL) return;
  let keyFile = process.env.FIREBASE_AUTH_KEYFILE;
  if (!keyFile) return;
  keyFile = keyFile.replace(/'/g, '"');

  let serviceAccount = JSON.parse(keyFile);
  try {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: databaseURL
    });
    console.log("Firebase initialized...");
  } catch (error) {
    console.log("Error initializing firebase", error.message);
  }
}

const myFunc = async () => {
  await initFirebase();

  let client = await MongoClient.connect(dburl, {
    useUnifiedTopology: true
  });
  db = client.db();
  console.log("Syncing firestore variants to mongo...");

  const synced = await syncFireStoreAreaBasketToMongo();
  console.log("Synced", synced);
  return true
};

const syncFireStoreAreaBasketToMongo = async () => {
  console.log("syncFireStoreAreaBasketToMongo...");
  // Get the variants by hexCode
  const db = admin.firestore();
  let hexCodes = [];

  try {
    const documents = await db.collection("areabaskets").listDocuments();

    documents.forEach(document => {
      hexCodes.push(document.id)
    });
  } catch (error) {
    console.log("Error occurred while getting hexCodes", error.message)
  }

  console.log(`Fetched ${hexCodes.length} AreaBaskets from firestore...`);

  const chunks = _.chunk(hexCodes, 50);
  _.each(chunks, chunk => {
    _.each(chunk, async hexCode => {
      let variants = [];
      let domains;
      try {
        domains = await db.collection("areabaskets").doc(hexCode).collection('variants').get();
        domains.forEach(domain => {
          variants.push(domain.data())
        })
        await createAreaBasketVariants(hexCode, variants)
      } catch (error) {
        console.log(`Error occured syncing firestore to mongo`, error.message)
      }
    })
  })
  return true;
}

const createAreaBasketVariants = async (plus6HexCode, docs) => {
  console.log("createAreaBasketVariants", plus6HexCode);

  const variantIds = _.pluck(docs, "variantId");
  let productVariants = [];
  try {
    productVariants = await db.collection("productvariants").aggregate([
      { $match: { _id: { $in: variantIds } } },
      {
        $lookup: {
          from: "products",
          localField: "productId",
          foreignField: "_id",
          as: "productDetail"
        }
      },
      {
        $unwind: "$productDetail"
      }
    ]).toArray();
  } catch (error) {
    console.log("Error ocurred", error.message);
  }

  // loop through docs, find the variantId
  _.map(docs, sData => {
    const uVariant = {
      ..._.find(productVariants, pVariant => pVariant._id === sData.variantId),
      ...sData
    }

    const areabasketVariant = buildAreaBasketVariant(plus6HexCode, uVariant)
    // Validate schema here
    let areaBasketVariantContext = AreaBasketVariant.namedContext("areabasketvariants");
    areaBasketVariantContext.validate(areabasketVariant);
    if (areabasketVariant.price > 0) {
      if (areaBasketVariantContext.isValid()) {
        try {
          db.collection("areabasketvariants").findOneAndUpdate({ variantHexCode: `${areabasketVariant.variantId}_${areabasketVariant.hexCode}` },
            { $set: areabasketVariant },
            { upsert: true });
        } catch (error) {
          console.log("Error inserting variant", error.message)
        }
      } else {
        console.log('AreaBasketVariant is not Valid!', areaBasketVariantContext.validationErrors());
      }
    }
  })
  return true;
}

const buildAreaBasketVariant = (plus6HexCode, productVariant) => {
  return {
    _id: Random(18, 'aA0'),
    brandId: productVariant.productDetail.brandId || "",
    brandName: productVariant.productDetail.brand || "",
    category: productVariant.category || "",
    categoryGroupId: productVariant.categoryGroupId || "",
    categoryId: productVariant.categoryId || "",
    code: productVariant.code,
    currency: productVariant.currency,
    dateAdded: new Date(),
    description: productVariant.description,
    enabled: productVariant.enabled !== undefined ? productVariant.enabled : true,
    isFeatured: productVariant.isFeatured,
    name: productVariant.name,
    hexCode: plus6HexCode,
    price: productVariant.customerGroupPrices ? productVariant.customerGroupPrices.ROT : productVariant.price,
    productCreatedAt: productVariant.productDetail.createdAt || "",
    productId: productVariant.productId,
    productName: productVariant.productDetail.name,
    subUnit: {
      upc: productVariant.subUnit ? productVariant.subUnit.upc : "",
      syncedAt: new Date()
    },
    tags: productVariant.productDetail.tags,
    unitDescription: productVariant.unitDescription,
    variantCreatedAt: productVariant.createdAt,
    variantId: productVariant._id
  };
}

myFunc();
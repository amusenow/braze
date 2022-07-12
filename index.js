import request from 'request'
import rp from 'request-promise-native';
import { Router } from 'express'
import { apiStatus, apiError } from '../../../lib/util'
import { adjustIndexName } from '../../../lib/elastic'

module.exports = ({ config, db }) => {
  let brazeApi = Router()

  const extractProductsData = (products) => products.map(({ _source: product }) => ({
    'external_id': config.extensions.braze.userId || '123',
    'app_id': config.extensions.braze.appId,
    'product_id': product.name || '',
    'currency': 'USD',
    'price': product.price || 0,
    'time': new Date(Date.now()).toISOString(),
    'properties': {
      'Allow Message': product.gift_message_available || false,
      'Open Amount': product.allow_open_amount || false,
      'Categories': product.category && product.category
        ? product.category.map(category => category.name || '')
        : [],
      'Classification': product.classification || '',
      'Cost': product.cost || '',
      'Description': product.description || '',
      'Email Template': product.giftcard_email_template || '',
      'Amount': product.giftcard_amounts || 0,
      'Card Type': product.giftcard_type || '',
      'Manufacturer': product.manufacturer || '',
      'Minimal Price': product.msrp || 0,
      'Product Name': product.name || '',
      'Price': product.price || 0,
      'Dynamic Price': product.dynamic_price || 0,
      'Price View': product.price_view || '',
      'Quantity': product.stock && product.stock.qty ? product.stock.qty : 1,
      'SKU': product.sku || '',
      'Dynamic SKU': product.sku_type || '',
      'Special Price From Date': product.special_from_date || 0,
      'Special Price': product.special_price || 0,
      'Special Price To Date': product.special_to_date || 0,
      'Treez Amount': product.treez_amount || 1,
      'Treez Category Name': product.treez_category || '',
      'Treez Product Id': product.treez_product_id || '',
      'Treez Product Ids': product.treez_product_ids || '',
      'Treez Sub Category Name': product.treez_sub_category || '',
      'Use Config Email Template': product.use_config_email_template || '',
      'Use Config Is Redeemable': product.use_config_is_redeemable || '',
      'Use Config Lifetime': product.use_config_lifetime || '',
      'Weight': product.weight || 0,
      'Dynamic Weight': product.dynamic_weight || 0
    }
  }))

  const exportToBraze = (res, products) => rp({
    uri: config.extensions.braze.apiUrl,
    method: 'POST',
    json:{
      purchases: extractProductsData(products)
    },
    headers: {
      'Authorization': `Bearer ${config.extensions.braze.apiKey}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    }
  })
    .then(res => res)
    .catch(err => apiError(res, err))

  const splitPrpducts = (products) => {
    const size = 10
    let splittedProducts = []

    for (let i = 0; i < products.length; i += size) {
      splittedProducts.push(products.slice(i, i + size))
    }

    return splittedProducts
  }

  const exportProducts = (req, res) => {
    const storeCode = req.query.storeCode ? req.query.storeCode.toLowerCase() : '*'
    const indexName = `store_data_${storeCode}`
    let elasticBackendUrl = `${config.elasticsearch.host}:${config.elasticsearch.port}/${adjustIndexName(indexName, 'product', config)}/_search`

    if (!elasticBackendUrl.startsWith('http')) {
      elasticBackendUrl = `${config.elasticsearch.protocol}://${elasticBackendUrl}`
    }

    request({
      uri: elasticBackendUrl,
      method: req.method,
      body: {
        size: 5000
      },
      json: true,
      auth: null
    }, (_err, _res, _resBody) => {
      if (_err || _resBody.error) {
        console.error(_err || _resBody.error)
        return apiError(res, _err || _resBody.error)
      }

      try {
        if (
          _resBody &&
          _resBody.hits &&
          _resBody.hits.hits &&
          _resBody.hits.hits.length
        ) {
          const promises = splitPrpducts(_resBody.hits.hits).map(products => exportToBraze(res, products))

          Promise.all(promises)
            .then(result => apiStatus(res, result, 200))
            .catch(err => apiError(res, err))
        }
      } catch (err) {
        apiError(res, err)
      }
    })
  }

  brazeApi.get('/export-products', (req, res) => {
    exportProducts(req, res)
  })

  return brazeApi
}

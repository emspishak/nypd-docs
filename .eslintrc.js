module.exports = {
  'env': {
    'node': true,
    'commonjs': true,
    'es2021': true,
  },
  'extends': [
    'eslint:recommended',
    'google',
  ],
  'parserOptions': {
    'ecmaVersion': 'latest',
  },
  'rules': {
    'valid-jsdoc': 'off',
  },
};

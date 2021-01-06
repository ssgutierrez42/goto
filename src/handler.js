'use strict'

const ActivitySampleGranular = require('@celled/models.activity.granular');
const ActivitySampleDaily = require('@celled/models.activity.daily');
const ActivitySampleWalk = require('@celled/models.activity.walk');
const ActivitySampleWeekly = require('@celled/models.activity.weekly');
const Learner = require('@celled/models.learner');

const aggregationController = require('./controllers/aggregationController');

const connectTo = 'mongodb://192.168.7.152:27017/dbtestcelled';
ActivitySampleGranular.connect(connectTo);
ActivitySampleDaily.connect(connectTo);
ActivitySampleWalk.connect(connectTo);
ActivitySampleWeekly.connect(connectTo);
Learner.connect(connectTo);

module.exports = async (event, context) => {
    console.log(`[service.activity.tracking]()`);

    aggregationController.handleCreateActivityWalkAndWeeklySamples();
    return context
        .status(200)
        .succeed();
}

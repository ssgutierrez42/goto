'use strict';

const Promise = require('bluebird');
const { DateTime } = require('luxon');
const _ = require('lodash');
const moment = require('moment');
const logger = require('../config/logger');

const ActivitySampleGranular = require('@celled/models.activity.granular');
const ActivitySampleDaily = require('@celled/models.activity.daily');
const ActivitySampleWalk = require('@celled/models.activity.walk');
const ActivitySampleWeekly = require('@celled/models.activity.weekly');
const Learner = require('@celled/models.learner');

/**
 * Return the most recent aggregate activity record for
 * @param {Learner} and
 * @param {type} - ActivitySampleDaily.type
 */
const getLastActivityDailyRecord = async function (learner, type) {
	return await Promise.resolve(ActivitySampleDaily.findOne({ learner: learner, type: type }).sort({ date: -1 }).select('date').lean().exec());
}

/**
 * Return the most recent granular activity record for
 * @param {Learner} and
 * @param {type} - ActivitySampleGranular.type
 */
const getLastActivityGranularRecord = async function (learner, type) {
	return await Promise.resolve(ActivitySampleGranular.findOne({ learner: learner, type: type }).sort({ startDate: -1 }).select('timezone').lean().exec());
}

/**
 * Given a set of granular samples, group them and create/update daily aggregate results.
 * This is meant to aggregate samples from the same learner (!).
 * @param {Array<ActivitySampleGranular>} (lean)
 *   - ALL entries must have the same learner, type, unit, appVersion, and timezone.
 *     otherwise, things will explode.
 */
const createDailySamplesForType = async function(granularSamples) {

	var resultMap = {};
	var metadataSource = granularSamples[0]; // use first entry for activty metadata, since all will be consistent.
	var creationCount = 0;
	for (const granularSample of granularSamples) {

		if (metadataSource.type !== granularSample.type) {
			throw new Error('All samples must be of the same type.');
		}

		const timezone = granularSample.timezone ? granularSample.timezone : 'GMT';
		const startDate = DateTime.fromJSDate(granularSample.startDate).setZone(timezone).startOf('day');
		const startDateString = startDate.toISO(); // toLocaleString(DateTime.DATE_SHORT);
		const source = granularSample.source;

		if (_.isEmpty(resultMap[source])) {
			resultMap[source] = {};
			resultMap[source][startDateString] = granularSample.value;
		} else {
			const currentValue = resultMap[source][startDateString] || 0;
			resultMap[source][startDateString] = currentValue + granularSample.value;  // aggregate
		}
	}

	for (const source of Object.keys(resultMap)) {

		for (const date of Object.keys(resultMap[source])) {

			const value = resultMap[source][date];
			const findRecord = {
				date: date, // supplied as string
				source: source,

				type: metadataSource.type,
				unit: metadataSource.unit,
				learner: metadataSource.learner,
				appVersion: metadataSource.appVersion,
				timezone: metadataSource.timezone,
			}

			const createRecord = {
				...findRecord,
				value: value
			}

			try {
				const existingRecord = await Promise.resolve(ActivitySampleDaily.findOne(findRecord));
				if (existingRecord) {
					existingRecord.value = value;
					await Promise.resolve(existingRecord.savePromise());
				} else {
					await Promise.resolve(ActivitySampleDaily.create(createRecord));
				}

				creationCount++;
			} catch (error) {
				logger.error(`[createDailySamplesForType] unable to create daily sample: ${error}`);
			}

		}
	}

	logger.debug(`[createDailySamplesForType] created/updated ${creationCount} records`);
}

/**
 * Given a learner, find their most recent granular activity samples.
 * This function looks for samples received in the last 3 days.
 * Samples are then queued over for aggregation by createDailySamplesForType
 * @param {Learner}
 * @param {retroactiveDays} - minimum look-back threshold. Not all data arrives at once.
 */
const createDailyActivitySamplesForLearner = async function(learner, retroactiveDays = 3) {
	const types = await Promise.resolve(ActivitySampleGranular.find({ learner: learner }).distinct('type').lean().exec());

	for (const type of types) {

		const lastActivityDailyRecord = await getLastActivityDailyRecord(learner, type); // used to get last date
		const lastActivityGranularRecord = await getLastActivityGranularRecord(learner, type) // used to get timezone

		var timezone = 'GMT';
		if (lastActivityGranularRecord && lastActivityGranularRecord.timezone) {
			timezone = lastActivityGranularRecord.timezone;
		}

		var queryDate = DateTime.local().setZone(timezone).minus({ days: retroactiveDays }).startOf('day').toJSDate();
		if (lastActivityDailyRecord && lastActivityDailyRecord.date) {
			// run AT LEAST the last three days continuously
			queryDate = queryDate < lastActivityDailyRecord.date ? queryDate : lastActivityDailyRecord.date;
		}

		const valuesQuery = {
			learner: learner,
			type: type,
			startDate: {
				$gt: queryDate,
				$lt: Date.now()
	    	}
		};

		try {
			const granularSamples = await Promise.resolve(ActivitySampleGranular.find(valuesQuery).lean().exec());
			logger.debug(`[createDailyActivitySamplesForLearner] aggregating ${granularSamples.length} sample(s) for type ${type}`);
			await createDailySamplesForType(granularSamples);
		} catch (error) {
			logger.error(`[createDailyActivitySamplesForLearner] unable to create daily samples: ${error}`);
		}

	}
}

/**
 * This function looks for aggregate walk records that have not been populated,
 * it then looks for aggregate granular records that were stored around the same time,
 * and populates the walk record with the total steps, distance, & stairs for that walk.
 */
const populateWalkActivitySamples = async function() {
	/* **************************************************************
	*  1 - incomplete records have a walk start and end date, but isPopulated = false
	************************************************************* */
	const query = {
		startDate: { $ne: null },
		endDate: { $ne: null },
		isPopulated: { $ne: true }
	};
	const incompleteWalkRecords = await ActivitySampleWalk.find(query).exec();
	logger.debug(`[populateWalkActivitySamples] populating ${incompleteWalkRecords.length} record(s)`);
	for (const walk of incompleteWalkRecords) {

		const aggregate = [
			{ $match: { learner: walk.learner } },
			{ $match: { endDate: { $gt: walk.startDate } } },
			{ $match: { startDate: { $lt: walk.endDate } } },
			{ $group: { _id: '$type', total: { $sum: '$value' } } }
		];

		/* **************************************************************
		*  2 - look for records during the time of the walk, sort by type
		************************************************************* */
		const granularSamples = await ActivitySampleGranular.aggregate(aggregate).exec();

		/* **************************************************************
		*  3 - update the walk record with relevant aggregate results
		************************************************************* */
		for (const sample of granularSamples) {
			const type = sample._id;
			const value = sample.total;

			switch (type) {
				case ActivitySampleGranular.ENUM_ACTIVITY_SAMPLE_TYPE.STEP_COUNT:
					walk.steps = value;
					break;
				case ActivitySampleGranular.ENUM_ACTIVITY_SAMPLE_TYPE.DISTANCE_WALKING_OR_RUNNING:
					walk.distance = value;
					break;
				case ActivitySampleGranular.ENUM_ACTIVITY_SAMPLE_TYPE.FLIGHTS_CLIMBED:
					walk.stairs = value;
					break;
			}
		}

		walk.isPopulated = true;
		await walk.save();
	}
}

/**
 * Creates all needed weekly aggregate records for the given learner.
 * This results in ActivitySampleWeekly object(s) with an assigned
 * week, start, and end date. Activity data is populated in a different call.
 * @param {Learner}
 */
const createWeeklyActivitySamplesForLearner = async function(learnerLean) {
	const learner = await Learner.findOne({ _id: learnerLean._id }).exec();
	if (!learner) return;

	const joinedDate = learner.getJoinedDateFromConsent();
	const joinedDateMoment = moment(joinedDate);
	const currentDateMoment = moment();

	const weeksSinceJoined = currentDateMoment.clone().diff(joinedDateMoment, 'weeks');

	const lastWeekSampleForLearner = await ActivitySampleWeekly.findOne({ learner: learner }).sort('-createdAt').lean().exec();
	const lastRecordedWeek = lastWeekSampleForLearner ? lastWeekSampleForLearner.week : -1;

	if (lastRecordedWeek === weeksSinceJoined) { 
		logger.debug(`[createWeeklyActivitySamplesForLearner] learner ${learner._id} has all of their ${weeksSinceJoined} weekly sample(s) already`);
		return; // all done, no need to record anything
	}

	logger.debug(`[createWeeklyActivitySamplesForLearner] learner ${learner._id} joined on ${joinedDateMoment}, and the current date is ${currentDateMoment}`);
	logger.debug(`[createWeeklyActivitySamplesForLearner] creating ${weeksSinceJoined - lastRecordedWeek} records for learner ${learner._id}`);

	let creationCount = 0;
	const nextWeekToRecord = lastRecordedWeek + 1;
	// go from the next week to record, to the latest week since the learner joined
	for (let i = nextWeekToRecord; i <= weeksSinceJoined; i++) {
		const startDate = joinedDateMoment.clone().add(i, 'weeks');
		const endDate = joinedDateMoment.clone().add(i + 1, 'weeks');

		const findRecord = {
			week: i,
			learner: learner
		};

		const createRecord = {
			...findRecord,
			startDate: startDate.toDate(),
			endDate: endDate.toDate()
		};

		try {
			const existingRecord = await ActivitySampleWeekly.findOne(findRecord)
			if (!existingRecord) {
				await ActivitySampleWeekly.create(createRecord);
			}

			creationCount++;
		} catch (error) {
			logger.error(`[createWeeklyActivitySamplesForLearner] unable to create weekly sample: ${error}`);
		}

	}

	logger.debug(`[createWeeklyActivitySamplesForLearner] CREATED/UPDATED ${creationCount} records for learner ${learner._id}`);
}

/**
 * Look for weekly samples that are not populated, 
 * then query against ActivitySampleGranular and ActivitySampleWalk
 * to find totals and averages !
 */
const populateWeeklyActivitySamples = async function() {
	/* **************************************************************
	*  1 - incomplete records have a walk start and end date, but isPopulated = false
	************************************************************* */
	const query = {
		startDate: { $ne: null },
		endDate: { $ne: null },
		week: { $ne: null },
		isPopulated: { $ne: true }
	};
	const incompleteWeeklyRecords = await ActivitySampleWeekly.find(query).exec();
	logger.debug(`[populateWeeklyActivitySamples] populating ${incompleteWeeklyRecords.length} record(s)`);
	for (const week of incompleteWeeklyRecords) {

		let sharedAggregate = [
			{ $match: { learner: week.learner } },
			{ $match: { endDate: { $gt: week.startDate } } },
			{ $match: { startDate: { $lt: week.endDate } } }
		];

		/* **************************************************************
		*  1 - find GRANULAR aggregate samples (PASSIVE)
		************************************************************* */
		const granularAggregate = [
			...sharedAggregate,
			{ $group: { _id: '$type', total: { $sum: '$value' }, average: { $avg: '$value' } } }
		];
		const granularSamples = await ActivitySampleGranular.aggregate(granularAggregate).exec();
		for (const sample of granularSamples) {
			const type = sample._id;
			const value = sample.total;
			const average = sample.average;

			switch (type) {
				case ActivitySampleGranular.ENUM_ACTIVITY_SAMPLE_TYPE.STEP_COUNT:
					week.totalPassiveSteps = value;
					week.avgPassiveSteps = average;
					break;
				case ActivitySampleGranular.ENUM_ACTIVITY_SAMPLE_TYPE.DISTANCE_WALKING_OR_RUNNING:
					week.totalPassiveDistance = value;
					week.avgPassiveDistance = average;
					break;
				case ActivitySampleGranular.ENUM_ACTIVITY_SAMPLE_TYPE.FLIGHTS_CLIMBED:
					week.totalPassiveStairs = value;
					week.avgPassiveStairs = average;
					break;
			}
		}

		/* **************************************************************
		*  2 - find walk aggregate samples (OPEN WALK AND 6MWT)
		************************************************************* */
		const walkAggregate = [
			...sharedAggregate,
			{ $match: { isPopulated: true } },
			{ 
				$group: { 
					_id: '$walkType', 
					totalSteps: { $sum: '$steps' },
					totalDistance: { $sum: '$distance' },
					totalStairs: { $sum: '$stairs' },
					totalDuration: { $sum: '$duration' },
					totalWalks: { $sum: 1 },
					averageSteps: { $avg: '$steps' },
					averageDistance: { $avg: '$distance' },
					averageStairs: { $avg: '$stairs' },
					averageDuration: { $avg: '$duration' },
				} 
			}
		];

		const walkSamples = await ActivitySampleWalk.aggregate(walkAggregate).exec();
		for (const sample of walkSamples) {
			const walkType = sample._id;

			switch (walkType) {
				case ActivitySampleWalk.ENUM_WALK_TYPE.WALK_OPEN:
					week.totalWalkSteps = sample.totalSteps;
					week.totalWalkDistance = sample.totalDistance;
					week.totalWalkStairs = sample.totalStairs;
					week.totalWalkTime = sample.totalDuration;
					week.totalWalks = sample.totalWalks;
					week.avgWalkSteps = sample.averageSteps;
					week.avgWalkDistance = sample.averageDistance;
					week.avgWalkStairs = sample.averageStairs;
					week.avgWalkTime = sample.averageDuration;
					break;
				case ActivitySampleWalk.ENUM_WALK_TYPE.WALK_6MWT:
					week.totalWalkTestSteps = sample.totalSteps;
					week.totalWalkTestDistance = sample.totalDistance;
					week.totalWalkTestStairs = sample.totalStairs;
					week.totalWalkTestTime = sample.totalDuration;
					week.totalWalkTests = sample.totalWalks;
					week.avgWalkTestSteps = sample.averageSteps;
					week.avgWalkTestDistance = sample.averageDistance;
					week.avgWalkTestStairs = sample.averageStairs;
					week.avgWalkTestTime = sample.averageDuration;
					break;
			}
		}

		week.isPopulated = true;
		await week.save();
	}
}

/**
 * For every walk that has ever been taken on the Cell-Ed platform,
 * aggregate the results and create a weekly report table
 */
const createActivityWalkAndWeeklySamples = async function() {
	// find and populate incomplete walk records
	await populateWalkActivitySamples();

	// get all learners who have granular entries
	const learners = await Promise.resolve(ActivitySampleGranular.find().select('learner').distinct('learner').lean().exec());
	logger.debug(`[createActivityWalkAndWeeklySamples] creating samples for ${learners.length} learner(s).`);
	for (const learner of learners) {
		// create their weekly aggregate records
		await createWeeklyActivitySamplesForLearner(learner);
	}

	// find and populate incomplete weekly records
	await populateWeeklyActivitySamples();
}

/**
 * For every distinct learner that has granular activity data,
 * Aggregate their results and store them as daily insights.
 */
const createActivitySamples = async function() {
	// get all learners who have granular entries
	const learners = await Promise.resolve(ActivitySampleGranular.find().select('learner').distinct('learner').lean().exec());
	logger.debug(`[createActivitySamples] creating samples for ${learners.length} learner(s).`);
	for (const learner of learners) {
		await createDailyActivitySamplesForLearner(learner);
	}
}

exports.handleCreateActivityDailySamples = async function(req, res) {
	createActivitySamples(); // create asynchronously.
	if (res) {
		res.status(200).send({});
	}
}

exports.handleCreateActivityWalkAndWeeklySamples = async function(req, res) {
	createActivityWalkAndWeeklySamples(); // create asynchronously.
	if (res) {
		res.status(200).send({});
	}
}

exports.createActivityWalkAndWeeklySamples = createActivityWalkAndWeeklySamples;

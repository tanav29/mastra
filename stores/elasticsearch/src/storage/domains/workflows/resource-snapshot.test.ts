import { Client } from '@elastic/elasticsearch';
import { TABLE_WORKFLOW_SNAPSHOT } from '@mastra/core/storage';
import { afterAll, expect, it } from 'vitest';

import { ElasticSearchDB } from '../../db';
import { WorkflowsElasticSearch } from './index';

const sharedClient = new Client({ node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200' });
afterAll(() => sharedClient.close());

it('loads a resource-suffixed legacy run and deletes both copies after updating it', async () => {
  const workflows = new WorkflowsElasticSearch({ client: sharedClient });
  const workflowName = 'legacy-snapshot';
  const runId = crypto.randomUUID();
  const resourceId = 'legacy-resource';
  const namespace = 'workflows';
  const snapshot = { status: 'suspended', context: {} };
  await workflows.init();
  const key =
    TABLE_WORKFLOW_SNAPSHOT +
    ':namespace:workflows:workflow_name:' +
    workflowName +
    ':run_id:' +
    runId +
    ':resourceId:' +
    resourceId;
  const createdAt = new Date().toISOString();
  await new ElasticSearchDB({ client: sharedClient }).set({
    tableName: TABLE_WORKFLOW_SNAPSHOT,
    key,
    value: {
      namespace,
      workflow_name: workflowName,
      run_id: runId,
      resourceId,
      snapshot,
      createdAt,
      updatedAt: createdAt,
    },
  });

  try {
    expect(await workflows.loadWorkflowSnapshot({ namespace, workflowName, runId })).toEqual(snapshot);
    await workflows.updateWorkflowState({ workflowName, runId, opts: { status: 'running' } });
    const { runs } = await workflows.listWorkflowRuns({ workflowName, resourceId });
    expect(runs.filter(run => run.runId === runId)).toMatchObject([
      { snapshot: { status: 'running' }, createdAt: new Date(createdAt) },
    ]);
  } finally {
    await workflows.deleteWorkflowRunById({ workflowName, runId });
  }
  expect(await workflows.getWorkflowRunById({ workflowName, runId })).toBeNull();
});

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
/ -->

<script setup lang="ts">
import { computed, onMounted, reactive, shallowReactive, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { message } from 'ant-design-vue'
import type { ColumnProps } from 'ant-design-vue/es/table'
import type { IMap, IColumns } from '@/types/common.type'
import { getTableDetail, updateTableProperties } from '@/services/table.service'

const { t } = useI18n()
const route = useRoute()

const propertiesColumns: IColumns[] = shallowReactive([
  { title: t('key'), dataIndex: 'key', width: '40%', ellipsis: true },
  { title: t('value'), dataIndex: 'value', width: '50%', ellipsis: true },
  { title: t('action'), dataIndex: 'action', width: '10%', align: 'center' },
])

const params = computed(() => {
  return {
    ...route.query,
  }
})

watch(
  () => route.query,
  (val) => {
    val?.catalog && route.path === '/tables' && getTablePropertiesData()
  },
)

const state = reactive({
  loading: false,
  saving: false,
  properties: [] as IMap<string>[],
  originalProperties: {} as IMap<string>,
  editedProperties: {} as IMap<string>,
  deletedProperties: [] as string[],
  newProperty: { key: '', value: '' },
})

async function getTablePropertiesData() {
  try {
    const { catalog, db, table } = params.value
    if (!catalog || !db || !table) {
      return
    }
    state.loading = true
    const result = await getTableDetail({
      catalog,
      db,
      table,
    })

    const properties = result.properties || {}
    state.originalProperties = { ...properties }
    state.editedProperties = { ...properties }
    state.deletedProperties = []

    state.properties = Object.keys(properties).map((key) => {
      return {
        key,
        value: properties[key],
      }
    })
  }
  catch (error) {
    message.error('failed Save Table Properties')
  }
  finally {
    state.loading = false
  }
}

function onPropertyChange(key: string, value: string) {
  state.editedProperties[key] = value
}

function onNewPropertyChange(field: 'key' | 'value', value: string) {
  state.newProperty[field] = value
}

function addNewProperty() {
  if (state.newProperty.key && state.newProperty.value) {
    state.editedProperties[state.newProperty.key] = state.newProperty.value
    state.properties.push({
      key: state.newProperty.key,
      value: state.newProperty.value,
    })
    state.newProperty = { key: '', value: '' }
  }
}

function removeProperty(key: string) {
  delete state.editedProperties[key]
  state.properties = state.properties.filter(prop => prop.key !== key)

  if (key in state.originalProperties && !state.deletedProperties.includes(key)) {
    state.deletedProperties.push(key)
  }
}

async function saveProperties() {
  try {
    const { catalog, db, table } = params.value
    if (!catalog || !db || !table) {
      return
    }

    state.saving = true

    const changedProperties: IMap<string> = {}

    // Check for modified properties
    Object.keys(state.editedProperties).forEach(key => {
      if (state.editedProperties[key] !== state.originalProperties[key]) {
        changedProperties[key] = state.editedProperties[key]
      }
    })

    // Include new properties that don't exist in original
    Object.keys(state.editedProperties).forEach(key => {
      if (!(key in state.originalProperties)) {
        changedProperties[key] = state.editedProperties[key]
      }
    })

    // Include deleted properties (set to null or empty string to indicate deletion)
    state.deletedProperties.forEach(key => {
      changedProperties[key] = ''
    })

    if (Object.keys(changedProperties).length === 0) {
      message.info('No Properties Changes For Table')
      return
    }

    await updateTableProperties({
      catalog,
      db,
      table,
      properties: changedProperties,
    })

    state.originalProperties = { ...state.editedProperties }
    state.deletedProperties = []
    message.success('After Table Properties Successful')
  }
  catch (error) {
    message.error('failed Save Table Properties')
  }
  finally {
    state.saving = false
  }
}

onMounted(() => {
  getTablePropertiesData()
})

defineExpose({ getTablePropertiesData })
</script>

<template>
  <div class="properties-view">
    <div class="properties-header">
      <h3>{{ t('tableProperties') }}</h3>
      <a-button
        type="primary"
        :loading="state.saving"
        @click="saveProperties"
      >
        {{ t('save') }}
      </a-button>
    </div>

    <div class="properties-content">
      <a-table
        :columns="propertiesColumns"
        :data-source="state.properties"
        :pagination="false"
        :loading="state.loading"
        row-key="key"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.dataIndex === 'value'">
            <a-input
              :value="state.editedProperties[record.key]"
              @change="(e: any) => onPropertyChange(record.key, e.target.value)"
              :placeholder="t('enterPropertyValue')"
            />
          </template>
          <template v-if="column.dataIndex === 'action'">
            <a-button
              type="text"
              size="small"
              @click="removeProperty(record.key)"
              class="delete-btn"
            >
              -
            </a-button>
          </template>
        </template>
      </a-table>

      <div class="add-property-row">
        <a-input
          v-model:value="state.newProperty.key"
          :placeholder="t('enterPropertyKey')"
          @change="(e: any) => onNewPropertyChange('key', e.target.value)"
          class="property-key-input"
        />
        <a-input
          v-model:value="state.newProperty.value"
          :placeholder="t('enterPropertyValue')"
          @change="(e: any) => onNewPropertyChange('value', e.target.value)"
          class="property-value-input"
        />
        <a-button
          type="primary"
          :disabled="!state.newProperty.key || !state.newProperty.value"
          @click="addNewProperty"
          class="add-property-btn"
        >
          +
        </a-button>
      </div>
    </div>
  </div>
</template>

<style lang="less" scoped>
.properties-view {
  padding: 24px;

  .properties-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;

    h3 {
      margin: 0;
      font-size: 18px;
      font-weight: bold;
      color: #102048;
    }
  }

  .properties-content {
    background: #fff;
    border-radius: 6px;
    padding: 24px;

    :deep(.ant-table-wrapper) {
      .ant-table {
        border: 1px solid #f0f0f0;
        border-radius: 6px;

        .ant-table-thead > tr > th {
          background: #fafafa;
          font-weight: 600;
          color: #102048;
        }

        .ant-table-tbody > tr > td {
          padding: 12px 16px;
        }
      }
    }

    .add-property-row {
      display: flex;
      gap: 12px;
      align-items: center;
      margin-top: 16px;
      padding-top: 16px;
      border-top: 1px solid #f0f0f0;

      .property-key-input {
        flex: 1;
      }

      .property-value-input {
        flex: 2;
      }

      .add-property-btn {
        min-width: 32px;
        height: 32px;
        padding: 0 8px;
        font-size: 16px;
        font-weight: bold;
      }
    }

    .delete-btn {
      min-width: 24px;
      height: 24px;
      padding: 0 4px;
      font-size: 16px;
      font-weight: bold;
      border-radius: 4px;
      color: #1890ff;

      &:hover {
        background: rgba(24, 144, 255, 0.1);
      }
    }
  }
}
</style>
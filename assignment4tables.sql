<panther-dialog
  header="Upstream Lineage"
  class="panther-dialog"
  [(visible)]="showPopup"
  [width]="900"
  [minWidth]="200"
  [minY]="70"
  [baseZIndex]="10000"
  [modal]="true"
  [responsive]="true"
  [closable]="true"
  [draggable]="true"
  (onHide)="closePopup()"
>
  <div dialog-content>
    <div class="tabs">
      <div
        *ngFor="let detail of transformationDetails; let i = index"
        [class.active]="selectedTabIndex === i"
        (click)="selectTab(i)"
        class="tab"
      >
        Transformation {{ detail.transformationID }}
      </div>
    </div>

    <div class="details-container" *ngIf="transformationDetails[selectedTabIndex]">
      <div class="detail-item">
        <span class="detail-label">Reporting Requirement:</span>
        <span class="detail-value">{{ transformationDetails[selectedTabIndex].reportingReq || 'N/A' }}</span>
      </div>
      <div class="detail-item">
        <span class="detail-label">Transformation Type:</span>
        <span class="detail-value">{{ transformationDetails[selectedTabIndex].transformationType || 'N/A' }}</span>
      </div>
      <div class="detail-item">
        <span class="detail-label">Asset Class:</span>
        <span class="detail-value">{{ transformationDetails[selectedTabIndex].assetclass || 'N/A' }}</span>
      </div>
      <div class="detail-item">
        <span class="detail-label">Product Type:</span>
        <span class="detail-value">{{ transformationDetails[selectedTabIndex].producttype || 'N/A' }}</span>
      </div>
    </div>

    <h3>Input Fields</h3>
    <table class="input-fields-table">
      <thead>
        <tr>
          <th>Data Source</th>
          <th>Field ID</th>
          <th>Field Name</th>
        </tr>
      </thead>
      <tbody>
        <tr *ngFor="let field of transformationDetails[selectedTabIndex]?.inputFieldDataList">
          <td>{{ field.dataSource }}</td>
          <td>{{ field.dependentfieldID }}</td>
          <td>{{ field.xpath }}</td>
        </tr>
      </tbody>
    </table>
  </div>

  <div dialog-footer>
    <button (click)="closePopup()" class="close-button">Close</button>
  </div>
</panther-dialog>

// MIT License
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package v1

import (
	"fmt"
	"github.com/microsoft/frameworkcontroller/pkg/common"
	core "k8s.io/api/core/v1"
	"os"
	"reflect"
	"time"
)

func init() {
	initCompletionCodeInfos()
}

///////////////////////////////////////////////////////////////////////////////////////
// General Constants
///////////////////////////////////////////////////////////////////////////////////////
const (
	// For controller
	ComponentName      = "frameworkcontroller"
	GroupName          = "frameworkcontroller.microsoft.com"
	Version            = "v1"
	FrameworkPlural    = "frameworks"
	FrameworkCRDName   = FrameworkPlural + "." + GroupName
	FrameworkKind      = "Framework"
	ConfigMapKind      = "ConfigMap"
	PodKind            = "Pod"
	ObjectUIDFieldPath = "metadata.uid"

	ConfigFilePath         = "./frameworkcontroller.yaml"
	UnlimitedValue         = -1
	ExtendedUnlimitedValue = -2

	// For all managed objects
	// Predefined Annotations
	AnnotationKeyFrameworkNamespace = "FC_FRAMEWORK_NAMESPACE"
	AnnotationKeyFrameworkName      = "FC_FRAMEWORK_NAME"
	AnnotationKeyTaskRoleName       = "FC_TASKROLE_NAME"
	AnnotationKeyTaskIndex          = "FC_TASK_INDEX"
	AnnotationKeyConfigMapName      = "FC_CONFIGMAP_NAME"
	AnnotationKeyPodName            = "FC_POD_NAME"

	AnnotationKeyFrameworkAttemptID          = "FC_FRAMEWORK_ATTEMPT_ID"
	AnnotationKeyFrameworkAttemptInstanceUID = "FC_FRAMEWORK_ATTEMPT_INSTANCE_UID"
	AnnotationKeyConfigMapUID                = "FC_CONFIGMAP_UID"
	AnnotationKeyTaskAttemptID               = "FC_TASK_ATTEMPT_ID"

	// Predefined Labels
	LabelKeyFrameworkName = AnnotationKeyFrameworkName
	LabelKeyTaskRoleName  = AnnotationKeyTaskRoleName

	// For all managed containers
	// Predefined Environment Variables
	// It can be referred by other environment variables specified in the Container Env,
	// i.e. specify its value to include "$(AnyPredefinedEnvName)".
	// If the reference is predefined, it will be replaced to its target value when
	// start the Container, otherwise it will be unchanged.
	EnvNameFrameworkNamespace = AnnotationKeyFrameworkNamespace
	EnvNameFrameworkName      = AnnotationKeyFrameworkName
	EnvNameTaskRoleName       = AnnotationKeyTaskRoleName
	EnvNameTaskIndex          = AnnotationKeyTaskIndex
	EnvNameConfigMapName      = AnnotationKeyConfigMapName
	EnvNamePodName            = AnnotationKeyPodName

	EnvNameFrameworkAttemptID          = AnnotationKeyFrameworkAttemptID
	EnvNameFrameworkAttemptInstanceUID = AnnotationKeyFrameworkAttemptInstanceUID
	EnvNameConfigMapUID                = AnnotationKeyConfigMapUID
	EnvNameTaskAttemptID               = AnnotationKeyTaskAttemptID
	EnvNameTaskAttemptInstanceUID      = "FC_TASK_ATTEMPT_INSTANCE_UID"
	EnvNamePodUID                      = "FC_POD_UID"

	// For Pod Spec
	// Predefined Pod Template Placeholders
	// It can be referred in any string value specified in the Pod Spec,
	// i.e. specify the value to include "{{AnyPredefinedPlaceholder}}".
	// If the reference is predefined, it will be replaced to its target value when
	// create the Pod object, otherwise it will be unchanged.
	PlaceholderFrameworkNamespace = AnnotationKeyFrameworkNamespace
	PlaceholderFrameworkName      = AnnotationKeyFrameworkName
	PlaceholderTaskRoleName       = AnnotationKeyTaskRoleName
	PlaceholderTaskIndex          = AnnotationKeyTaskIndex
	PlaceholderConfigMapName      = AnnotationKeyConfigMapName
	PlaceholderPodName            = AnnotationKeyPodName
)

var FrameworkGroupVersionKind = SchemeGroupVersion.WithKind(FrameworkKind)
var ConfigMapGroupVersionKind = core.SchemeGroupVersion.WithKind(ConfigMapKind)
var PodGroupVersionKind = core.SchemeGroupVersion.WithKind(PodKind)

var ObjectUIDEnvVarSource = &core.EnvVarSource{
	FieldRef: &core.ObjectFieldSelector{FieldPath: ObjectUIDFieldPath},
}

var EnvValueKubeApiServerAddress = os.Getenv("KUBE_APISERVER_ADDRESS")
var EnvValueKubeConfigFilePath = os.Getenv("KUBECONFIG")
var DefaultKubeConfigFilePath = os.Getenv("HOME") + "/.kube/config"

///////////////////////////////////////////////////////////////////////////////////////
// CompletionCodeInfos
///////////////////////////////////////////////////////////////////////////////////////
// +k8s:deepcopy-gen=false
// Represent [Min, Max].
type CompletionCodeRange struct {
	Min CompletionCode
	Max CompletionCode
}

var CompletionCodeReservedPositive = CompletionCodeRange{200, 219}
var CompletionCodeReservedNonPositive = CompletionCodeRange{-999, 0}

const (
	// [200, 219]: Predefined Container ExitCode
	// It is Reserved for the Contract between Container and FrameworkController,
	// so Container should avoid unintendedly exit within the range.
	CompletionCodeContainerTransientFailed         CompletionCode = 200
	CompletionCodeContainerTransientConflictFailed CompletionCode = 201
	CompletionCodeContainerPermanentFailed         CompletionCode = 210

	// [0, 0]: Succeeded
	CompletionCodeSucceeded CompletionCode = 0

	// [-999, -1]: Predefined Framework Error
	// -1XX: Transient Error
	CompletionCodeConfigMapExternalDeleted        CompletionCode = -100
	CompletionCodePodExternalDeleted              CompletionCode = -101
	CompletionCodeConfigMapCreationTimeout        CompletionCode = -110
	CompletionCodePodCreationTimeout              CompletionCode = -111
	CompletionCodePodFailedWithoutFailedContainer CompletionCode = -120
	// -2XX: Permanent Error
	CompletionCodePodSpecInvalid             CompletionCode = -200
	CompletionCodeStopFrameworkRequested     CompletionCode = -210
	CompletionCodeFrameworkAttemptCompletion CompletionCode = -220
	// -3XX: Unknown Error
)

var completionCodeInfoList = []*CompletionCodeInfo{}
var completionCodeInfoMap = map[CompletionCode]*CompletionCodeInfo{}

var completionCodeInfoContainerUnrecognizedFailed = &CompletionCodeInfo{
	Phrase: "ContainerUnrecognizedFailed",
	Type:   CompletionType{CompletionTypeNameFailed, []CompletionTypeAttribute{}},
}

func initCompletionCodeInfos() {
	AppendCompletionCodeInfos([]*CompletionCodeInfo{
		{
			Code:   CompletionCodeContainerTransientFailed.Ptr(),
			Phrase: "ContainerTransientFailed",
			Type: CompletionType{
				CompletionTypeNameFailed, []CompletionTypeAttribute{
					CompletionTypeAttributeTransient}},
			PodPatterns: []*PodPattern{{
				Containers: []*ContainerPattern{{
					CodeRange: Int32Range{
						Min: common.PtrInt32(int32(CompletionCodeContainerTransientFailed)),
						Max: common.PtrInt32(int32(CompletionCodeContainerTransientFailed)),
					},
				}},
			}},
		},
		{
			Code:   CompletionCodeContainerTransientConflictFailed.Ptr(),
			Phrase: "ContainerTransientConflictFailed",
			Type: CompletionType{CompletionTypeNameFailed, []CompletionTypeAttribute{
				CompletionTypeAttributeTransient, CompletionTypeAttributeConflict}},
			PodPatterns: []*PodPattern{{
				Containers: []*ContainerPattern{{
					CodeRange: Int32Range{
						Min: common.PtrInt32(int32(CompletionCodeContainerTransientConflictFailed)),
						Max: common.PtrInt32(int32(CompletionCodeContainerTransientConflictFailed)),
					},
				}},
			}},
		},
		{
			Code:   CompletionCodeContainerPermanentFailed.Ptr(),
			Phrase: "ContainerPermanentFailed",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributePermanent}},
			PodPatterns: []*PodPattern{{
				Containers: []*ContainerPattern{{
					CodeRange: Int32Range{
						Min: common.PtrInt32(int32(CompletionCodeContainerPermanentFailed)),
						Max: common.PtrInt32(int32(CompletionCodeContainerPermanentFailed)),
					},
				}},
			}},
		},
		{
			Code:   CompletionCodeSucceeded.Ptr(),
			Phrase: "Succeeded",
			Type: CompletionType{CompletionTypeNameSucceeded,
				[]CompletionTypeAttribute{}},
		},
		{
			Code:   CompletionCodeConfigMapExternalDeleted.Ptr(),
			Phrase: "ConfigMapExternalDeleted",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributeTransient}},
		},
		{
			// Possibly due to Pod Eviction or Preemption.
			Code:   CompletionCodePodExternalDeleted.Ptr(),
			Phrase: "PodExternalDeleted",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributeTransient}},
		},
		{
			Code:   CompletionCodeConfigMapCreationTimeout.Ptr(),
			Phrase: "ConfigMapCreationTimeout",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributeTransient}},
		},
		{
			Code:   CompletionCodePodCreationTimeout.Ptr(),
			Phrase: "PodCreationTimeout",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributeTransient}},
		},
		{
			Code:   CompletionCodePodFailedWithoutFailedContainer.Ptr(),
			Phrase: "PodFailedWithoutFailedContainer",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributeTransient}},
		},
		{
			Code:   CompletionCodePodSpecInvalid.Ptr(),
			Phrase: "PodSpecInvalid",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributePermanent}},
		},
		{
			Code:   CompletionCodeStopFrameworkRequested.Ptr(),
			Phrase: "StopFrameworkRequested",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributePermanent}},
		},
		{
			Code:   CompletionCodeFrameworkAttemptCompletion.Ptr(),
			Phrase: "FrameworkAttemptCompletion",
			Type: CompletionType{CompletionTypeNameFailed,
				[]CompletionTypeAttribute{CompletionTypeAttributePermanent}},
		},
	})
}

func AppendCompletionCodeInfos(codeInfos []*CompletionCodeInfo) {
	for _, codeInfo := range codeInfos {
		if existingCodeInfo, ok := completionCodeInfoMap[*codeInfo.Code]; ok {
			// Unreachable
			panic(fmt.Errorf(
				"Failed to append CompletionCodeInfo due to duplicated CompletionCode:"+
					"\nExisting CompletionCodeInfo:\n%v,\nAppending CompletionCodeInfo:\n%v",
				common.ToYaml(existingCodeInfo), common.ToYaml(codeInfo)))
		}

		completionCodeInfoList = append(completionCodeInfoList, codeInfo)
		completionCodeInfoMap[*codeInfo.Code] = codeInfo
	}
}

// +k8s:deepcopy-gen=false
type PodMatchResult struct {
	CodeInfo    *CompletionCodeInfo
	Diagnostics string
}

// +k8s:deepcopy-gen=false
type PodMatchDiagnostics struct {
	Name                string `json:"name,omitempty"`
	PodCompletionStatus `json:",inline"`
}

// Match ANY CompletionCodeInfo
func MatchCompletionCodeInfos(pod *core.Pod) PodMatchResult {
	for _, codeInfo := range completionCodeInfoList {
		for _, podPattern := range codeInfo.PodPatterns {
			if diag := matchPodPattern(pod, podPattern); diag != nil {
				return PodMatchResult{codeInfo, *diag}
			}
		}
	}

	// ALL CompletionCodeInfos cannot be matched, fall back to unmatched result.
	return generatePodUnmatchedResult(pod)
}

// Match ENTIRE PodPattern
func matchPodPattern(pod *core.Pod, podPattern *PodPattern) *string {
	matchedPod := &PodMatchDiagnostics{}

	if ms := podPattern.NameRegex.FindString(pod.Name); ms != nil {
		if !podPattern.NameRegex.IsZero() {
			matchedPod.Name = *ms
		}
	} else {
		return nil
	}
	if ms := podPattern.ReasonRegex.FindString(pod.Status.Reason); ms != nil {
		if !podPattern.ReasonRegex.IsZero() {
			matchedPod.Reason = *ms
		}
	} else {
		return nil
	}
	if ms := podPattern.MessageRegex.FindString(pod.Status.Message); ms != nil {
		if !podPattern.MessageRegex.IsZero() {
			matchedPod.Message = *ms
		}
	} else {
		return nil
	}

	containers := GetAllContainerStatuses(pod)
	for _, containerPattern := range podPattern.Containers {
		if mc := matchContainers(containers, containerPattern); mc != nil {
			if !reflect.DeepEqual(*mc, ContainerCompletionStatus{}) {
				matchedPod.Containers = append(matchedPod.Containers, mc)
			}
		} else {
			return nil
		}
	}

	return common.PtrString(fmt.Sprintf(
		"PodPattern matched: %v", common.ToJson(matchedPod)))
}

// Match ANY Container
func matchContainers(
	containers []core.ContainerStatus,
	containerPattern *ContainerPattern) *ContainerCompletionStatus {
	for _, container := range containers {
		if mc := matchContainerPattern(container, containerPattern); mc != nil {
			return mc
		}
	}

	return nil
}

// Match ENTIRE ContainerPattern
func matchContainerPattern(
	container core.ContainerStatus,
	containerPattern *ContainerPattern) *ContainerCompletionStatus {
	term := container.State.Terminated
	matchedContainer := ContainerCompletionStatus{}

	if ms := containerPattern.NameRegex.FindString(container.Name); ms != nil {
		if !containerPattern.NameRegex.IsZero() {
			matchedContainer.Name = *ms
		}
	} else {
		return nil
	}
	if ms := containerPattern.ReasonRegex.FindString(term.Reason); ms != nil {
		if !containerPattern.ReasonRegex.IsZero() {
			matchedContainer.Reason = *ms
		}
	} else {
		return nil
	}
	if ms := containerPattern.MessageRegex.FindString(term.Message); ms != nil {
		if !containerPattern.MessageRegex.IsZero() {
			// Escape it in case multiple lines.
			matchedContainer.Message = common.ToJson(*ms)
		}
	} else {
		return nil
	}

	if containerPattern.SignalRange.Contains(term.Signal) {
		if !containerPattern.SignalRange.IsZero() {
			matchedContainer.Signal = term.Signal
		}
	} else {
		return nil
	}
	if containerPattern.CodeRange.Contains(term.ExitCode) {
		if !containerPattern.CodeRange.IsZero() {
			matchedContainer.Code = term.ExitCode
		}
	} else {
		return nil
	}

	return &matchedContainer
}

func generatePodUnmatchedResult(pod *core.Pod) PodMatchResult {
	// Take the last failed Container ExitCode as CompletionCode and full failure
	// info as Diagnostics.
	lastContainerExitCode := common.NilInt32()
	lastContainerCompletionTime := time.Time{}
	for _, containerStatus := range GetAllContainerStatuses(pod) {
		term := containerStatus.State.Terminated
		if term != nil {
			if term.ExitCode != 0 && (lastContainerExitCode == nil ||
				lastContainerCompletionTime.Before(term.FinishedAt.Time)) {
				lastContainerExitCode = &term.ExitCode
				lastContainerCompletionTime = term.FinishedAt.Time
			}
		}
	}

	unmatchedPod := ExtractPodCompletionStatus(pod)
	diag := fmt.Sprintf("PodPattern unmatched: %v", common.ToJson(unmatchedPod))
	if lastContainerExitCode == nil {
		return PodMatchResult{
			CodeInfo:    completionCodeInfoMap[CompletionCodePodFailedWithoutFailedContainer],
			Diagnostics: diag,
		}
	} else {
		return PodMatchResult{
			CodeInfo: &CompletionCodeInfo{
				Code:   (*CompletionCode)(lastContainerExitCode),
				Phrase: completionCodeInfoContainerUnrecognizedFailed.Phrase,
				Type:   completionCodeInfoContainerUnrecognizedFailed.Type,
			},
			Diagnostics: diag,
		}
	}
}
